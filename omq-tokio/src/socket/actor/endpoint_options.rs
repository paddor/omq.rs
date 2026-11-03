//! Per-endpoint options: capture at bind or connect, and setup checks.

use omq_proto::Options;
use std::sync::Arc;

use super::{Endpoint, Error, Result, SocketDriver, SocketType};

impl SocketDriver {
    pub(super) fn capture_endpoint_options(
        &self,
        compression: Option<omq_proto::CompressionOptions>,
    ) -> Arc<Options> {
        let mut options = self.options.clone();
        options.recv_payload_pool = self.resources.payload_pools.receive();
        if let Some(compression) = compression {
            options.compression_dict = compression.dict;
            options.compression_auto_train = compression.auto_train;
            options.compression_threshold = compression.threshold;
            options.compression_level = compression.level;
            options.compression_level_range = compression.level_range;
            options.compression_dict_capacity = compression.dict_capacity;
        }
        if let Some(dict) = &options.compression_dict
            && dict.len() <= 8192
        {
            options.compression_dict = Some(bytes::Bytes::copy_from_slice(dict));
        }
        Arc::new(options)
    }

    pub(super) fn validate_setup_options(
        &self,
        endpoint: &Endpoint,
        options: &Options,
    ) -> Result<()> {
        options.validate()?;
        if matches!(endpoint, Endpoint::Shm(_)) {
            #[cfg(not(target_os = "linux"))]
            return Err(Error::UnsupportedScheme("shm requires Linux".into()));
            #[cfg(target_os = "linux")]
            {
                if !omq_shm::supports(self.socket_type) {
                    return Err(Error::Config(
                        "shm:// does not support STREAM sockets".into(),
                    ));
                }
                if !matches!(options.mechanism, omq_proto::MechanismConfig::Null) {
                    return Err(Error::Config("shm:// requires the NULL mechanism".into()));
                }
            }
        }
        #[cfg(feature = "dart")]
        if matches!(endpoint, Endpoint::Dart { .. }) {
            return self.validate_dart_options(options);
        }
        // Validate the effective mechanism before DNS or carrier setup. The
        // codec factory repeats this check for standalone sans-I/O callers.
        let compression =
            omq_proto::proto::transform::CompressionKind::for_endpoint_with_mechanism(
                endpoint,
                &options.mechanism,
            )?;
        if compression.is_some() && self.socket_type == SocketType::Stream {
            return Err(Error::Config(
                "OMQ compression is not supported on STREAM sockets".into(),
            ));
        }
        let _ = crate::engine::codec::CodecSetup::for_endpoint(endpoint, options)
            .map_err(|error| Error::Config(format!("invalid codec configuration: {error}")))?;
        #[cfg(feature = "ws")]
        if endpoint.is_ws_family() {
            match endpoint {
                Endpoint::Ws { host, path, .. } | Endpoint::Wss { host, path, .. } => {
                    omq_proto::proto::ws_handshake::validate_ws_address(host, path)?;
                }
                #[cfg(feature = "lz4")]
                Endpoint::Lz4Ws { host, path, .. } => {
                    omq_proto::proto::ws_handshake::validate_ws_address(host, path)?;
                }
                _ => unreachable!(),
            }
            if options
                .handshake_timeout
                .and_then(|timeout| std::time::Instant::now().checked_add(timeout))
                .is_none()
            {
                return Err(Error::Protocol(
                    "WS/WSS requires a finite handshake_timeout".into(),
                ));
            }
        }
        #[cfg(not(feature = "ws"))]
        let _ = endpoint;
        Ok(())
    }
}
