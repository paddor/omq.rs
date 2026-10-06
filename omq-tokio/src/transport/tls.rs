//! Verified TLS configuration shared by encrypted carriers.
//!
//! No insecure verification mode here. WSS keeps its explicit test override
//! in its own module; later carriers must choose their policy independently.

use omq_proto::endpoint::Host;
use omq_proto::{Error, Result};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};

pub(crate) fn install_provider() {
    // Respect a provider installed by the embedding application.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub(crate) fn verified_client_config(
    trust_system: bool,
    trust_pem: Option<&[u8]>,
) -> Result<rustls::ClientConfig> {
    install_provider();
    let mut roots = rustls::RootCertStore::empty();
    if trust_system {
        let cert_result = rustls_native_certs::load_native_certs();
        if cert_result.certs.is_empty() && !cert_result.errors.is_empty() {
            return Err(Error::Io(std::io::Error::other(format!(
                "failed to load system certificates: {:?}",
                cert_result.errors,
            ))));
        }
        for cert in cert_result.certs {
            let _ = roots.add(cert);
        }
    }
    if let Some(pem) = trust_pem {
        let mut found = false;
        for cert in CertificateDer::pem_slice_iter(pem) {
            let cert = cert.map_err(|e| Error::Protocol(format!("invalid trust PEM: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| Error::Protocol(format!("invalid trust certificate: {e}")))?;
            found = true;
        }
        if !found {
            return Err(Error::Protocol("trust PEM contains no certificates".into()));
        }
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

pub(crate) fn server_config(cert_pem: &[u8], key_pem: &[u8]) -> Result<rustls::ServerConfig> {
    install_provider();
    let certs: Vec<_> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::Protocol(format!("invalid cert PEM: {e}")))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|e| Error::Protocol(format!("invalid key PEM: {e}")))?;
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::Protocol(format!("TLS config: {e}")))
}

pub(crate) fn server_name(host: &Host, override_name: Option<&str>) -> Result<ServerName<'static>> {
    let name = match (override_name, host) {
        (Some(name), _) => name.to_string(),
        (None, Host::Name(name)) => name.clone(),
        (None, Host::Ip(ip)) => return Ok(ServerName::IpAddress((*ip).into())),
        _ => {
            return Err(Error::InvalidEndpoint(
                "TLS requires a concrete server name".into(),
            ));
        }
    };
    ServerName::try_from(name).map_err(|e| Error::Protocol(format!("invalid TLS server name: {e}")))
}

#[cfg(all(test, feature = "ws"))]
mod tests;
