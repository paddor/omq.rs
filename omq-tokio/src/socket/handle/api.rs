use super::{Socket, TrySendError};
use omq_proto::{
    endpoint::Endpoint,
    error::{Error, Result},
    message::Message,
    options::Options,
    proto::SocketType,
};

impl omq_proto::socket_api::SocketApi for Socket {
    fn new(socket_type: SocketType, options: Options) -> Self {
        Socket::new(socket_type, options)
    }
    fn socket_type(&self) -> SocketType {
        self.socket_type()
    }
    async fn bind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<Endpoint> {
        self.bind(endpoint).await
    }
    async fn connect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        self.connect(endpoint).await
    }
    async fn send(&self, msg: Message) -> Result<()> {
        self.send(msg).await
    }
    async fn recv(&self) -> Result<Message> {
        self.recv().await
    }
    fn try_send(&self, msg: Message) -> Result<()> {
        self.try_send(msg).map_err(|e| match e {
            TrySendError::Full(_) => Error::WouldBlock,
            TrySendError::Closed => Error::Closed,
            TrySendError::Error(e) => e,
        })
    }
    fn try_recv(&self) -> Result<Message> {
        self.try_recv()
    }
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.subscribe(prefix).await
    }
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.unsubscribe(prefix).await
    }
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.join(group).await
    }
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.leave(group).await
    }
    async fn unbind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        self.unbind(endpoint).await
    }
    async fn disconnect(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        self.disconnect(endpoint).await
    }
    async fn close(self) -> Result<()> {
        self.close().await
    }
}
