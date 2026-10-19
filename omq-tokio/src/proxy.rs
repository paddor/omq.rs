//! Socket proxy helper.
//!
//! A proxy composes two normal sockets. It adds no socket type and no
//! yring of its own. Existing socket HWM and routing rules remain in
//! force.
//!
//! The loop keeps at most one unsent message per direction when a target
//! reports HWM backpressure. A pending message waits in that target's own
//! send, which completes once the message is accepted, before the loop takes
//! more input from the same side. The configured burst size bounds how many
//! complete messages one hot side may forward before the other side and the
//! control socket get another chance to run.
//!
//! Steerable control accepts `PAUSE`, `RESUME`, `TERMINATE`, and `KILL`.
//! `KILL` is kept as an alias for callers that already use it. `STATISTICS`
//! is intentionally not implemented.

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::flow::DrainBudget;
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use crate::Socket;

/// Default max complete messages forwarded from one side per wake.
///
/// Larger bursts reduce select-loop overhead under load. Smaller bursts
/// reduce worst-case delay for the opposite direction and control socket.
pub const DEFAULT_PROXY_BURST_SIZE: usize = 64;

/// Bursts forwarded without awaiting before the loop yields its runtime
/// thread and lets the biased select poll control first.
const TURN_BURSTS: usize = 16;

/// Reason a proxy forwarding loop stopped.
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

type PendingSend = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

/// A forwarded message waiting for target space. The send future owns the
/// message and survives loop iterations; `copy` feeds the capture socket.
struct Pending {
    copy: Message,
    send: PendingSend,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending").finish_non_exhaustive()
    }
}

/// Forwarding state of both directions for one proxy run.
#[derive(Debug)]
struct Flows {
    fe_pending: Option<Pending>,
    be_pending: Option<Pending>,
    fe_to_be_enabled: bool,
    be_to_fe_enabled: bool,
    /// Tried first on the next turn; flips after each forward for fairness.
    preferred: Direction,
}

impl Flows {
    fn enabled(&self, direction: Direction) -> bool {
        match direction {
            Direction::FrontendToBackend => self.fe_to_be_enabled,
            Direction::BackendToFrontend => self.be_to_fe_enabled,
        }
    }

    fn pending(&mut self, direction: Direction) -> &mut Option<Pending> {
        match direction {
            Direction::FrontendToBackend => &mut self.fe_pending,
            Direction::BackendToFrontend => &mut self.be_pending,
        }
    }
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
        let mut flows = Flows {
            fe_pending: None,
            be_pending: None,
            fe_to_be_enabled: self.can_forward_direction(Direction::FrontendToBackend, false),
            be_to_fe_enabled: self.can_forward_direction(Direction::BackendToFrontend, same_socket),
            preferred: Direction::FrontendToBackend,
        };
        let mut state = ProxyState::Active;
        let mut turn = DrainBudget::new(TURN_BURSTS, usize::MAX);

        'main: loop {
            if let Some(action) = self.try_handle_control(&mut state).await?
                && action == ControlAction::Terminate
            {
                return Ok(ProxyExit::Terminated);
            }

            if state == ProxyState::Active {
                for direction in [flows.preferred, flows.preferred.opposite()] {
                    if self.try_forward_available(direction, &mut flows)? {
                        flows.preferred = direction.opposite();
                        if turn.account(0) {
                            continue 'main;
                        }
                        // Sources stay ready under sustained load, and socket
                        // receives never yield on their own.
                        turn.reset();
                        tokio::task::yield_now().await;
                        break;
                    }
                }
            }

            if let Some(exit) = self.wait_ready(&mut state, &mut flows).await? {
                return Ok(exit);
            }
        }
    }

    /// Park until control, a pending send, or a source is ready, then serve
    /// it. Returns the exit reason once the proxy must stop.
    async fn wait_ready(
        &self,
        state: &mut ProxyState,
        flows: &mut Flows,
    ) -> Result<Option<ProxyExit>> {
        let frontend = self.frontend.clone_shared();
        let backend = self.backend.clone_shared();
        let control = self.control.as_ref().map(Socket::clone_shared);
        let active = *state == ProxyState::Active;

        tokio::select! {
            biased;
            command = recv_optional(control), if self.control.is_some() => match command? {
                Some(command) => {
                    let action = self.handle_control(command, state).await?;
                    Ok((action == ControlAction::Terminate).then_some(ProxyExit::Terminated))
                }
                None => Ok(Some(ProxyExit::Closed)),
            },
            sent = pending_send(&mut flows.fe_pending),
                if active && flows.fe_to_be_enabled && flows.fe_pending.is_some() => {
                self.finish_pending(sent, &mut flows.fe_pending)?;
                flows.preferred = Direction::BackendToFrontend;
                Ok(None)
            }
            sent = pending_send(&mut flows.be_pending),
                if active && flows.be_to_fe_enabled && flows.be_pending.is_some() => {
                self.finish_pending(sent, &mut flows.be_pending)?;
                flows.preferred = Direction::FrontendToBackend;
                Ok(None)
            }
            msg = frontend.recv(),
                if active && flows.fe_to_be_enabled && flows.fe_pending.is_none() => {
                self.forward_burst(Direction::FrontendToBackend, msg?, &mut flows.fe_pending)?;
                flows.preferred = Direction::BackendToFrontend;
                Ok(None)
            }
            msg = backend.recv(),
                if active && flows.be_to_fe_enabled && flows.be_pending.is_none() => {
                self.forward_burst(Direction::BackendToFrontend, msg?, &mut flows.be_pending)?;
                flows.preferred = Direction::FrontendToBackend;
                Ok(None)
            }
        }
    }

    fn try_forward_available(&self, direction: Direction, flows: &mut Flows) -> Result<bool> {
        // A direction with a pending send takes no input until it completes.
        if !flows.enabled(direction) || flows.pending(direction).is_some() {
            return Ok(false);
        }
        let pending = flows.pending(direction);

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

    fn finish_pending(&self, sent: Result<()>, pending: &mut Option<Pending>) -> Result<()> {
        let copy = pending.take().expect("completed pending send").copy;
        sent?;
        if let Some(capture) = &self.capture {
            let _ = capture.try_send(copy);
        }
        Ok(())
    }

    fn pend(&self, direction: Direction, msg: Message) -> Pending {
        let target = self.target(direction).clone_shared();
        Pending {
            copy: msg.clone(),
            send: Box::pin(async move { target.send(msg).await }),
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
                    *pending = Some(self.pend(direction, returned));
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

/// Drive a pending send. Only polled while one exists.
fn pending_send(
    pending: &mut Option<Pending>,
) -> impl std::future::Future<Output = Result<()>> + '_ {
    std::future::poll_fn(move |cx| match pending {
        Some(pending) => pending.send.as_mut().poll(cx),
        None => std::task::Poll::Pending,
    })
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
