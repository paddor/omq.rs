use super::Socket;
use crate::socket::recv::BlockingRecvCancel;
use omq_proto::{
    error::{Error, Result},
    flow::DrainBudget,
    message::Message,
    proto::SocketType,
};

impl Socket {
    /// Blocking receive for sync callers. Register the caller and park until
    /// a valid complete message arrives.
    pub(crate) fn blocking_recv(&self) -> Result<Message> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self.inner.recv_rx.blocking_recv();
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            let message = self.inner.recv_rx.blocking_recv()?;
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(message);
            }
            if !budget.account(byte_len) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    pub(crate) fn blocking_recv_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self.inner.recv_rx.blocking_recv_cancelable(cancel);
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            let Some(message) = self.inner.recv_rx.blocking_recv_cancelable(cancel)? else {
                return Ok(None);
            };
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(Some(message));
            }
            if !budget.account(byte_len) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    #[inline]
    pub(crate) fn blocking_recv_registered_cancelable(
        &self,
        cancel: &BlockingRecvCancel,
    ) -> Result<Option<Message>> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self
                .inner
                .recv_rx
                .blocking_recv_registered_cancelable(cancel);
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            let Some(message) = self
                .inner
                .recv_rx
                .blocking_recv_registered_cancelable(cancel)?
            else {
                return Ok(None);
            };
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(Some(message));
            }
            if !budget.account(byte_len) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    /// Apply receive admission while respecting a dispatch-wide deadline.
    pub(crate) fn blocking_recv_for_dispatch(
        &self,
        deadline: Option<std::time::Instant>,
        cancel: Option<&BlockingRecvCancel>,
    ) -> Result<Option<Message>> {
        if deadline.is_none()
            && cancel.is_none()
            && !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep)
        {
            return self.inner.recv_rx.blocking_recv().map(Some);
        }
        let mut budget = DrainBudget::WORKER;
        loop {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::Timeout);
            }
            let received = if let Some(cancel) = cancel {
                self.inner
                    .recv_rx
                    .blocking_recv_registered_cancelable_until(cancel, deadline)?
            } else if let Some(deadline) = deadline {
                Some(self.inner.recv_rx.blocking_recv_until(deadline)?)
            } else {
                Some(self.inner.recv_rx.blocking_recv()?)
            };
            let Some(message) = received else {
                return Ok(None);
            };
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(Some(message));
            }
            if cancel.is_some_and(BlockingRecvCancel::is_canceled) {
                return Ok(None);
            }
            if !budget.account(byte_len) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    pub(crate) fn blocking_recv_timeout(&self, timeout: std::time::Duration) -> Result<Message> {
        if !matches!(self.inner.socket_type, SocketType::Req | SocketType::Rep) {
            return self.inner.recv_rx.blocking_recv_timeout(timeout);
        }
        let Some(deadline) = std::time::Instant::now().checked_add(timeout) else {
            return self.blocking_recv();
        };
        let mut budget = DrainBudget::WORKER;
        loop {
            let message = self.inner.recv_rx.blocking_recv_until(deadline)?;
            let byte_len = message.byte_len();
            if let Some(message) = self.prepare_external_recv(message) {
                return Ok(message);
            }
            if !budget.account(byte_len) {
                std::thread::yield_now();
                budget.reset();
            }
        }
    }

    pub(crate) fn blocking_recv_many(&self, max: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.blocking_recv_many_into(max, &mut messages)?;
        Ok(messages)
    }

    pub(crate) fn blocking_recv_many_into(
        &self,
        max: usize,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        if max == 0 {
            return Ok(0);
        }
        match self.try_recv_many_into(max, out) {
            Err(Error::WouldBlock) => {}
            result => return result,
        }
        let start_len = out.len();
        out.push(self.blocking_recv()?);
        self.try_recv_many_after_first(max, start_len, out)
    }

    pub(crate) fn blocking_recv_many_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        if max == 0 {
            return Ok(Some(0));
        }
        let start_len = out.len();
        let Some(message) = self.blocking_recv_cancelable(cancel)? else {
            return Ok(None);
        };
        out.push(message);
        self.try_recv_many_after_first(max, start_len, out)
            .map(Some)
    }

    #[inline]
    pub(crate) fn blocking_recv_many_registered_cancelable_into(
        &self,
        max: usize,
        cancel: &BlockingRecvCancel,
        out: &mut Vec<Message>,
    ) -> Result<Option<usize>> {
        if max == 0 {
            return Ok(Some(0));
        }
        let start_len = out.len();
        let Some(message) = self.blocking_recv_registered_cancelable(cancel)? else {
            return Ok(None);
        };
        out.push(message);
        self.try_recv_many_after_first(max, start_len, out)
            .map(Some)
    }

    pub(crate) fn blocking_recv_many_timeout(
        &self,
        max: usize,
        timeout: std::time::Duration,
    ) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max);
        self.blocking_recv_many_timeout_into(max, timeout, &mut messages)?;
        Ok(messages)
    }

    pub(crate) fn blocking_recv_many_timeout_into(
        &self,
        max: usize,
        timeout: std::time::Duration,
        out: &mut Vec<Message>,
    ) -> Result<usize> {
        if max == 0 {
            return Ok(0);
        }
        match self.try_recv_many_into(max, out) {
            Err(Error::WouldBlock) => {}
            result => return result,
        }
        let start_len = out.len();
        out.push(self.blocking_recv_timeout(timeout)?);
        self.try_recv_many_after_first(max, start_len, out)
    }
}
