//! Server-side WebTransport session requests over Quinn.

use url::Url;

use crate::{Connect, ServerError, Session, Settings};

/// A mostly complete WebTransport handshake awaiting server accept/reject based on URL.
pub struct Request {
    conn: Option<quinn::Connection>,
    settings: Option<Settings>,
    connect: Option<Connect>,
    url: Url,
}

impl Request {
    /// Accept a new WebTransport session from a client.
    pub async fn accept(conn: quinn::Connection) -> Result<Self, ServerError> {
        // Perform the HTTP/3 handshake by sending/receiving SETTINGS frames.
        let settings = Settings::connect(&conn, false).await?;

        // Accept the CONNECT request but defer the response.
        let connect = Connect::accept(&conn, settings.version()).await?;

        // Return the request while retaining settings/connect streams.
        let url = connect.url().clone();
        Ok(Self {
            conn: Some(conn),
            settings: Some(settings),
            connect: Some(connect),
            url,
        })
    }

    /// Return the URL provided by the client.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Origin supplied with the CONNECT request, if any.
    pub fn origin(&self) -> Option<&str> {
        self.connect.as_ref().and_then(Connect::origin)
    }

    /// Accept the session and return a 200 OK response.
    pub async fn ok(mut self) -> Result<Session, ServerError> {
        let mut connect = self
            .connect
            .take()
            .ok_or(ServerError::RequestAlreadyCompleted)?;
        connect.respond(http::StatusCode::OK).await?;
        let conn = self
            .conn
            .take()
            .ok_or(ServerError::RequestAlreadyCompleted)?;
        let settings = self
            .settings
            .take()
            .ok_or(ServerError::RequestAlreadyCompleted)?;
        Ok(Session::new(conn, settings, connect))
    }

    /// Reject the session and return the provided HTTP status code.
    pub async fn close(mut self, status: http::StatusCode) -> Result<(), ServerError> {
        let mut connect = self
            .connect
            .take()
            .ok_or(ServerError::RequestAlreadyCompleted)?;
        connect.reject(status).await?;
        Ok(())
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        let Some(mut connect) = self.connect.take() else {
            return;
        };

        // Keep the handshake resources alive until the explicit response has
        // been sent. A dropped request is a server-side failure, not a silent
        // cancellation, so use 500 rather than making the client wait for EOF.
        let conn = self.conn.take();
        let settings = self.settings.take();
        let url = self.url.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                %url,
                "dropped unanswered WebTransport request outside a Tokio runtime; \
                 unable to send the automatic rejection"
            );
            return;
        };

        runtime.spawn(async move {
            if let Err(error) = connect
                .reject(http::StatusCode::INTERNAL_SERVER_ERROR)
                .await
            {
                tracing::warn!(
                    %url,
                    %error,
                    "failed to send automatic rejection for dropped WebTransport request"
                );
            } else {
                tracing::warn!(
                    %url,
                    status = http::StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    "automatically rejected dropped unanswered WebTransport request"
                );
            }
            drop((conn, settings));
        });
    }
}
