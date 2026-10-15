use super::Socket;
use crate::socket::actor::SocketCommand;
use futures::channel::oneshot;
use omq_proto::{
    endpoint::Endpoint,
    error::{Error, Result},
};

impl Socket {
    /// Bind a URI string or typed endpoint. Returns the resolved endpoint once
    /// the listener is active. For wildcard binds (`tcp://...:0`), the returned
    /// endpoint contains the actual port.
    pub async fn bind(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
    ) -> Result<Endpoint> {
        self.bind_with_codec_snapshot(endpoint.try_into().map_err(Into::into)?, None)
            .await
    }

    /// Bind a URI string or typed endpoint with a compression-parameter snapshot.
    /// The endpoint selects the codec; `None` parameter fields use codec
    /// defaults. Existing listeners/connections and decoder limits retain their
    /// configuration. CURVE and unsupported carrier profiles remain rejected.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, bind failure, or a closed socket.
    pub async fn bind_with_compression_options(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        compression: omq_proto::CompressionOptions,
    ) -> Result<Endpoint> {
        self.bind_with_codec_snapshot(endpoint.try_into().map_err(Into::into)?, Some(compression))
            .await
    }

    async fn bind_with_codec_snapshot(
        &self,
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
    ) -> Result<Endpoint> {
        self.inner.payload_pools.freeze();
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Bind {
                endpoint,
                compression,
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        let resolved = rx.await.map_err(|_| Error::Closed)??;
        *self.inner.last_bound_endpoint.write().unwrap() = Some(resolved.clone());
        Ok(resolved)
    }

    /// Return the most recently bound endpoint, if any.
    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner.last_bound_endpoint.read().unwrap().clone()
    }

    /// Last STREAM route allocated by connect or accept. Connecting routes
    /// accept bounded sends before TCP is established; reconnect allocates a
    /// new identity. Used by the C API's `ZMQ_IDENTITY` option.
    #[doc(hidden)]
    pub async fn last_stream_routing_id(&self) -> Result<Option<bytes::Bytes>> {
        if self.socket_type() != omq_proto::SocketType::Stream {
            return Err(Error::Protocol("expected a STREAM socket".into()));
        }
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::QueryStreamRoutingId { ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Queue a connect attempt to a URI string or typed endpoint.
    /// The background reconnect loop handles retries per `ReconnectPolicy`.
    pub async fn connect(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
    ) -> Result<()> {
        self.connect_with_codec_snapshot(endpoint.try_into().map_err(Into::into)?, None)
            .await
    }

    /// Connect a URI string or typed endpoint with a compression snapshot.
    /// The snapshot is retained across retries and reconnects. The endpoint
    /// selects the codec kind. Existing peers retain their settings;
    /// this does not reconfigure a live dictionary.
    ///
    /// # Errors
    /// Returns an error for invalid local configuration or a closed socket.
    pub async fn connect_with_compression_options(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
        compression: omq_proto::CompressionOptions,
    ) -> Result<()> {
        self.connect_with_codec_snapshot(
            endpoint.try_into().map_err(Into::into)?,
            Some(compression),
        )
        .await
    }

    async fn connect_with_codec_snapshot(
        &self,
        endpoint: Endpoint,
        compression: Option<omq_proto::CompressionOptions>,
    ) -> Result<()> {
        self.inner.payload_pools.freeze();
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Connect {
                endpoint,
                compression,
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Subscribe to a topic prefix. Only valid on SUB / XSUB sockets; other
    /// types return `Error::Protocol`. An empty prefix subscribes to all
    /// topics. Sends a ZMTP SUBSCRIBE command to every currently-connected
    /// publisher and is replayed to new publishers on connect.
    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Subscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Cancel a previously-registered subscription prefix. No-op if the
    /// prefix wasn't subscribed.
    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unsubscribe {
                prefix: prefix.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Join a group. Only valid on DISH sockets. Sends a ZMTP JOIN command
    /// to every connected RADIO peer; replayed to new peers on connect.
    pub async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Join {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Leave a previously-joined group. No-op if not joined.
    pub async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Leave {
                group: group.into(),
                ack,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-established bind. Cancels the listener's
    /// accept loop and releases its socket file (filesystem IPC) without
    /// closing already-accepted peers. Returns `Error::Unroutable` if
    /// no listener at `endpoint` is registered.
    /// Accepts a URI string or typed endpoint.
    pub async fn unbind(&self, endpoint: impl TryInto<Endpoint, Error: Into<Error>>) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Unbind { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Tear down a previously-started connect. Cancels the dial loop,
    /// any in-flight reconnect backoff, and live peers connected through
    /// `endpoint`. Returns `Error::Unroutable` if no dialer or live peer
    /// at `endpoint` is registered. Accepts a URI string or typed endpoint.
    pub async fn disconnect(
        &self,
        endpoint: impl TryInto<Endpoint, Error: Into<Error>>,
    ) -> Result<()> {
        let endpoint = endpoint.try_into().map_err(Into::into)?;
        let (ack, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(SocketCommand::Disconnect { endpoint, ack })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
}
