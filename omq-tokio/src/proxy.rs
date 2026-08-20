//! Socket proxy helper.
//!
//! A proxy composes two normal sockets. It adds no socket type and no
//! yring of its own. Existing socket HWM and routing rules remain in
//! force.
//!
//! The loop keeps at most one unsent message per direction when a target
//! reports HWM backpressure. It retries that pending message before taking
//! more input from the same side. The configured burst size bounds how many
//! complete messages one hot side may forward before the other side and the
//! control socket get another chance to run.
//!
//! Steerable control accepts `PAUSE`, `RESUME`, `TERMINATE`, and `KILL`.
//! `KILL` is kept as an alias for callers that already use it. `STATISTICS`
//! is intentionally not implemented.

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use crate::Socket;

/// Default max complete messages forwarded from one side per wake.
///
/// Larger bursts reduce select-loop overhead under load. Smaller bursts
/// reduce worst-case delay for the opposite direction and control socket.
pub const DEFAULT_PROXY_BURST_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyExit {
    /// A control message requested termination.
    Terminated,
    /// A proxied socket closed.
    Closed,
}

/// Forward messages between two sockets.
///
/// `Proxy` owns only socket handles plus optional capture/control sockets.
/// It does not allocate a forwarding queue. Capture is best-effort: a full
/// capture socket drops the copied message and never backpressures the data
/// path.
#[derive(Debug)]
pub struct Proxy {
    frontend: Socket,
    backend: Socket,
    capture: Option<Socket>,
    control: Option<Socket>,
    burst_size: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    FrontendToBackend,
    BackendToFrontend,
}

impl Direction {
    fn opposite(self) -> Self {
        match self {
            Self::FrontendToBackend => Self::BackendToFrontend,
            Self::BackendToFrontend => Self::FrontendToBackend,
        }
    }
}

#[derive(Debug)]
struct Pending {
    msg: Message,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyState {
    Active,
    Paused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlAction {
    Continue,
    Terminate,
}

enum ForwardAttempt {
    Sent,
    Full(Message),
    Closed,
}

impl Proxy {
    /// Create a proxy that forwards between `frontend` and `backend`.
    pub fn new(frontend: Socket, backend: Socket) -> Self {
        Self {
            frontend,
            backend,
            capture: None,
            control: None,
            burst_size: DEFAULT_PROXY_BURST_SIZE,
        }
    }

    /// Add a best-effort capture socket.
    ///
    /// Each forwarded message is cloned to this socket when it has capacity.
    /// Capture backpressure never blocks the proxy data path.
    #[must_use]
    pub fn capture(mut self, socket: Socket) -> Self {
        self.capture = Some(socket);
        self
    }

    /// Set steerable control socket.
    ///
    /// A REP control socket receives an empty reply after each command,
    /// matching the pyzmq/libzmq request-reply control shape.
    #[must_use]
    pub fn control(mut self, socket: Socket) -> Self {
        self.control = Some(socket);
        self
    }

    /// Set max messages drained from one direction before rechecking peer
    /// direction and control.
    #[must_use]
    pub fn burst_size(mut self, burst_size: usize) -> Self {
        self.burst_size = burst_size.max(1);
        self
    }

    /// Run the proxy until a control command terminates it or a socket closes.
    pub async fn run(self) -> Result<ProxyExit> {
        match self.run_loop().await {
            Err(Error::Closed) => Ok(ProxyExit::Closed),
            result => result,
        }
    }

    async fn run_loop(self) -> Result<ProxyExit> {
        let same_socket = self.frontend.same_socket(&self.backend);
        let fe_to_be_enabled = self.can_forward_direction(Direction::FrontendToBackend, false);
        let be_to_fe_enabled =
            self.can_forward_direction(Direction::BackendToFrontend, same_socket);
        let mut state = ProxyState::Active;
        let mut fe_pending: Option<Pending> = None;
        let mut be_pending: Option<Pending> = None;
        let mut preferred = Direction::FrontendToBackend;

        'main: loop {
            if let Some(action) = self.try_handle_control(&mut state).await?
                && action == ControlAction::Terminate
            {
                return Ok(ProxyExit::Terminated);
            }

            if state == ProxyState::Active {
                for direction in [preferred, preferred.opposite()] {
                    if self.flush_pending_direction(
                        direction,
                        &mut fe_pending,
                        &mut be_pending,
                        fe_to_be_enabled,
                        be_to_fe_enabled,
                    )? {
                        preferred = direction.opposite();
                        continue 'main;
                    }
                    if self.try_forward_available(
                        direction,
                        &mut fe_pending,
                        &mut be_pending,
                        fe_to_be_enabled,
                        be_to_fe_enabled,
                    )? {
                        preferred = direction.opposite();
                        continue 'main;
                    }
                }
            }

            let fe_wait_msg = fe_pending.as_ref().map(|pending| pending.msg.clone());
            let be_wait_msg = be_pending.as_ref().map(|pending| pending.msg.clone());
            let frontend = self.frontend.clone();
            let backend = self.backend.clone();
            let control = self.control.clone();

            tokio::select! {
                biased;
                command = recv_optional(control), if self.control.is_some() => {
                    match command? {
                        Some(command) => {
                            if self.handle_control(command, &mut state).await?
                                == ControlAction::Terminate
                            {
                                return Ok(ProxyExit::Terminated);
                            }
                        }
                        None => return Ok(ProxyExit::Closed),
                    }
                }
                () = wait_for_send_progress(backend.clone(), fe_wait_msg),
                    if state == ProxyState::Active && fe_to_be_enabled && fe_pending.is_some() => {}
                () = wait_for_send_progress(frontend.clone(), be_wait_msg),
                    if state == ProxyState::Active && be_to_fe_enabled && be_pending.is_some() => {}
                msg = frontend.recv(),
                    if state == ProxyState::Active && fe_to_be_enabled && fe_pending.is_none() => {
                    let msg = msg?;
                    self.forward_burst(Direction::FrontendToBackend, msg, &mut fe_pending)?;
                    preferred = Direction::BackendToFrontend;
                }
                msg = backend.recv(),
                    if state == ProxyState::Active && be_to_fe_enabled && be_pending.is_none() => {
                    let msg = msg?;
                    self.forward_burst(Direction::BackendToFrontend, msg, &mut be_pending)?;
                    preferred = Direction::FrontendToBackend;
                }
            }
        }
    }

    fn flush_pending_direction(
        &self,
        direction: Direction,
        fe_pending: &mut Option<Pending>,
        be_pending: &mut Option<Pending>,
        fe_to_be_enabled: bool,
        be_to_fe_enabled: bool,
    ) -> Result<bool> {
        if !direction_enabled(direction, fe_to_be_enabled, be_to_fe_enabled) {
            return Ok(false);
        }
        match direction {
            Direction::FrontendToBackend => self.flush_pending(direction, fe_pending),
            Direction::BackendToFrontend => self.flush_pending(direction, be_pending),
        }
    }

    fn try_forward_available(
        &self,
        direction: Direction,
        fe_pending: &mut Option<Pending>,
        be_pending: &mut Option<Pending>,
        fe_to_be_enabled: bool,
        be_to_fe_enabled: bool,
    ) -> Result<bool> {
        if !direction_enabled(direction, fe_to_be_enabled, be_to_fe_enabled) {
            return Ok(false);
        }
        match direction {
            Direction::FrontendToBackend if fe_pending.is_some() => return Ok(false),
            Direction::BackendToFrontend if be_pending.is_some() => return Ok(false),
            _ => {}
        }

        let pending = match direction {
            Direction::FrontendToBackend => fe_pending,
            Direction::BackendToFrontend => be_pending,
        };

        match self.source(direction).try_recv() {
            Ok(msg) => {
                self.forward_burst(direction, msg, pending)?;
                Ok(true)
            }
            Err(Error::WouldBlock) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn can_forward_direction(&self, direction: Direction, same_socket: bool) -> bool {
        if same_socket && direction == Direction::BackendToFrontend {
            return false;
        }
        socket_can_recv(self.source(direction).socket_type())
            && socket_can_send(self.target(direction).socket_type())
    }

    fn flush_pending(&self, direction: Direction, pending: &mut Option<Pending>) -> Result<bool> {
        let Some(p) = pending.take() else {
            return Ok(false);
        };
        match self.try_forward(direction, p.msg)? {
            ForwardAttempt::Sent => Ok(true),
            ForwardAttempt::Full(msg) => {
                *pending = Some(Pending { msg });
                Ok(false)
            }
            ForwardAttempt::Closed => Err(Error::Closed),
        }
    }

    fn forward_burst(
        &self,
        direction: Direction,
        first: Message,
        pending: &mut Option<Pending>,
    ) -> Result<()> {
        let mut msg = first;
        for n in 0..self.burst_size {
            match self.try_forward(direction, msg)? {
                ForwardAttempt::Sent => {}
                ForwardAttempt::Full(returned) => {
                    *pending = Some(Pending { msg: returned });
                    return Ok(());
                }
                ForwardAttempt::Closed => return Err(Error::Closed),
            }

            if n + 1 == self.burst_size {
                return Ok(());
            }
            msg = match self.source(direction).try_recv() {
                Ok(next) => next,
                Err(Error::WouldBlock) => return Ok(()),
                Err(Error::Closed) => return Err(Error::Closed),
                Err(error) => return Err(error),
            };
        }
        Ok(())
    }

    fn try_forward(&self, direction: Direction, msg: Message) -> Result<ForwardAttempt> {
        let copy = msg.clone();
        let attempt = match self.target(direction).try_send(msg) {
            Ok(()) => {
                if let Some(capture) = &self.capture {
                    let _ = capture.try_send(copy);
                }
                ForwardAttempt::Sent
            }
            Err(TrySendError::Full(msg)) => ForwardAttempt::Full(msg),
            Err(TrySendError::Closed) => ForwardAttempt::Closed,
            Err(TrySendError::Error(error)) => return Err(error),
        };
        Ok(attempt)
    }

    fn source(&self, direction: Direction) -> &Socket {
        match direction {
            Direction::FrontendToBackend => &self.frontend,
            Direction::BackendToFrontend => &self.backend,
        }
    }

    fn target(&self, direction: Direction) -> &Socket {
        match direction {
            Direction::FrontendToBackend => &self.backend,
            Direction::BackendToFrontend => &self.frontend,
        }
    }

    async fn try_handle_control(&self, state: &mut ProxyState) -> Result<Option<ControlAction>> {
        let Some(control) = &self.control else {
            return Ok(None);
        };
        // REP try_recv would mutate REP state and then handle_control() would
        // send the ack through the wrong path. Let the normal recv branch own
        // REP control sockets.
        if control.socket_type() == SocketType::Rep {
            return Ok(None);
        }
        match control.try_recv() {
            Ok(msg) => self.handle_control(msg, state).await.map(Some),
            Err(Error::WouldBlock) => Ok(None),
            Err(Error::Closed) => Ok(Some(ControlAction::Terminate)),
            Err(error) => Err(error),
        }
    }

    async fn handle_control(&self, msg: Message, state: &mut ProxyState) -> Result<ControlAction> {
        let command = msg.part_bytes(0).unwrap_or_default();
        let action = match command.as_ref() {
            b"PAUSE" => {
                *state = ProxyState::Paused;
                ControlAction::Continue
            }
            b"RESUME" => {
                *state = ProxyState::Active;
                ControlAction::Continue
            }
            b"TERMINATE" | b"KILL" => ControlAction::Terminate,
            _ => ControlAction::Continue,
        };

        if let Some(control) = &self.control
            && control.socket_type() == SocketType::Rep
        {
            let _ = control.send(Message::single(bytes::Bytes::new())).await;
        }
        if action == ControlAction::Terminate {
            tokio::task::yield_now().await;
        }

        Ok(action)
    }
}

/// Run a non-steerable proxy between two sockets.
pub async fn proxy(
    frontend: Socket,
    backend: Socket,
    capture: Option<Socket>,
) -> Result<ProxyExit> {
    let mut proxy = Proxy::new(frontend, backend);
    if let Some(capture) = capture {
        proxy = proxy.capture(capture);
    }
    proxy.run().await
}

/// Run a steerable proxy between two sockets.
///
/// Control messages are `PAUSE`, `RESUME`, `TERMINATE`, and `KILL`.
pub async fn proxy_steerable(
    frontend: Socket,
    backend: Socket,
    capture: Option<Socket>,
    control: Option<Socket>,
) -> Result<ProxyExit> {
    let mut proxy = Proxy::new(frontend, backend);
    if let Some(capture) = capture {
        proxy = proxy.capture(capture);
    }
    if let Some(control) = control {
        proxy = proxy.control(control);
    }
    proxy.run().await
}

async fn recv_optional(socket: Option<Socket>) -> Result<Option<Message>> {
    let Some(socket) = socket else {
        std::future::pending::<()>().await;
        return Ok(None);
    };
    socket.recv().await.map(Some)
}

async fn wait_for_send_progress(socket: Socket, msg: Option<Message>) {
    let Some(msg) = msg else {
        std::future::pending::<()>().await;
        return;
    };
    // Space notifications are best-effort across multiple send strategies.
    // The short timer prevents a lost notify from pinning a pending message.
    tokio::select! {
        () = socket.wait_send_progress_for(&msg) => {}
        () = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
    }
}

fn direction_enabled(direction: Direction, fe_to_be_enabled: bool, be_to_fe_enabled: bool) -> bool {
    match direction {
        Direction::FrontendToBackend => fe_to_be_enabled,
        Direction::BackendToFrontend => be_to_fe_enabled,
    }
}

fn socket_can_recv(socket_type: SocketType) -> bool {
    !matches!(
        socket_type,
        SocketType::Pub | SocketType::Push | SocketType::Radio | SocketType::Scatter
    )
}

fn socket_can_send(socket_type: SocketType) -> bool {
    !matches!(
        socket_type,
        SocketType::Pull | SocketType::Sub | SocketType::Dish | SocketType::Gather
    )
}
