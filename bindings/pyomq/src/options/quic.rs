//! QUIC configuration is fixed before either backend is materialized.

use omq_proto::options::QuicOptions;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::{constants, int_to_bound};

pub(super) fn is_option(option: i32) -> bool {
    matches!(
        option,
        constants::OMQ_QUIC_CERT_PEM
            | constants::OMQ_QUIC_KEY_PEM
            | constants::OMQ_QUIC_TRUST_PEM
            | constants::OMQ_QUIC_SERVER_NAME
            | constants::OMQ_QUIC_TRUST_SYSTEM
            | constants::OMQ_QUIC_STREAM_WINDOW
            | constants::OMQ_QUIC_MAX_READY_PEERS
    )
}

pub(super) fn set(
    sock: &crate::socket::SocketInner,
    option: i32,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    // Materialization takes its slot lock before the overlay. Hold both read
    // guards through the update, so setup cannot capture an earlier snapshot.
    let asynchronous = sock.materialized.read().unwrap();
    let blocking = sock.blocking_materialized.read().unwrap();
    if asynchronous.is_some() || blocking.is_some() {
        return Err(crate::error::map_err(omq_proto::Error::Io(
            std::io::Error::from_raw_os_error(libc::EBUSY),
        )));
    }
    let mut overlay = sock.overlay.lock().unwrap();
    let quic = &mut overlay.quic;
    match option {
        constants::OMQ_QUIC_CERT_PEM
        | constants::OMQ_QUIC_KEY_PEM
        | constants::OMQ_QUIC_TRUST_PEM => {
            let bytes: &[u8] = value.extract()?;
            let pem = (!bytes.is_empty()).then(|| bytes.to_vec());
            match option {
                constants::OMQ_QUIC_CERT_PEM => quic.server_cert_pem = pem,
                constants::OMQ_QUIC_KEY_PEM => quic.server_key_pem = pem,
                _ => quic.trust_pem = pem,
            }
        }
        constants::OMQ_QUIC_SERVER_NAME => {
            let bytes: &[u8] = value.extract()?;
            let name = std::str::from_utf8(bytes).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("QUIC server name must be UTF-8")
            })?;
            quic.server_name = (!name.is_empty()).then(|| name.to_owned());
        }
        constants::OMQ_QUIC_TRUST_SYSTEM => quic.trust_system = value.extract::<i64>()? != 0,
        constants::OMQ_QUIC_STREAM_WINDOW => {
            let window = value.extract::<u32>()?;
            if !(QuicOptions::MIN_STREAM_WINDOW..=QuicOptions::MAX_STREAM_WINDOW).contains(&window)
            {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "QUIC stream window must be 16 KiB..=256 MiB",
                ));
            }
            quic.stream_window = window;
        }
        constants::OMQ_QUIC_MAX_READY_PEERS => {
            let peers = value.extract::<usize>()?;
            if peers == 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "QUIC ready peer limit must be positive",
                ));
            }
            quic.max_ready_peers = peers;
        }
        _ => unreachable!("checked QUIC option"),
    }
    Ok(())
}

pub(super) fn get<'py>(
    sock: &crate::socket::SocketInner,
    py: Python<'py>,
    option: i32,
) -> PyResult<Bound<'py, PyAny>> {
    let overlay = sock.overlay.lock().unwrap();
    let quic = &overlay.quic;
    let bytes =
        |pem: &Option<Vec<u8>>| Ok(PyBytes::new(py, pem.as_deref().unwrap_or_default()).into_any());
    match option {
        constants::OMQ_QUIC_CERT_PEM => bytes(&quic.server_cert_pem),
        constants::OMQ_QUIC_KEY_PEM => bytes(&quic.server_key_pem),
        constants::OMQ_QUIC_TRUST_PEM => bytes(&quic.trust_pem),
        constants::OMQ_QUIC_SERVER_NAME => Ok(PyBytes::new(
            py,
            quic.server_name.as_deref().unwrap_or_default().as_bytes(),
        )
        .into_any()),
        constants::OMQ_QUIC_TRUST_SYSTEM => Ok(int_to_bound(py, i64::from(quic.trust_system))),
        constants::OMQ_QUIC_STREAM_WINDOW => Ok(int_to_bound(py, i64::from(quic.stream_window))),
        constants::OMQ_QUIC_MAX_READY_PEERS => {
            Ok(quic.max_ready_peers.into_pyobject(py)?.into_any())
        }
        _ => unreachable!("checked QUIC option"),
    }
}
