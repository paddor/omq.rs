use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Identity {
    root: String,
    certificate: String,
    key: String,
}

fn identity(validity: Option<(i32, i32)>) -> Identity {
    let mut root = rcgen::CertificateParams::new(Vec::new()).unwrap();
    root.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    root.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    let root_key = rcgen::KeyPair::generate().unwrap();
    let root_cert = root.self_signed(&root_key).unwrap();
    let issuer = rcgen::Issuer::new(root, root_key);
    let mut leaf = rcgen::CertificateParams::new(vec!["service.example".into()]).unwrap();
    leaf.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    if let Some((before, after)) = validity {
        leaf.not_before = rcgen::date_time_ymd(before, 1, 1);
        leaf.not_after = rcgen::date_time_ymd(after, 1, 1);
    }
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let cert = leaf.signed_by(&leaf_key, &issuer).unwrap();
    Identity {
        root: root_cert.pem(),
        certificate: cert.pem(),
        key: leaf_key.serialize_pem(),
    }
}

async fn exchange(identity: &Identity, trust: Option<&[u8]>, name: &str) -> std::io::Result<()> {
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(
        server_config(identity.certificate.as_bytes(), identity.key.as_bytes()).unwrap(),
    ));
    let connector =
        tokio_rustls::TlsConnector::from(Arc::new(verified_client_config(false, trust).unwrap()));
    let (server, client) = tokio::io::duplex(16384);
    let (server, client) = tokio::join!(
        acceptor.accept(server),
        connector.connect(ServerName::try_from(name.to_string()).unwrap(), client),
    );
    let mut client = client?;
    let mut server = server?;
    client.write_all(b"verified").await?;
    client.flush().await?;
    let mut received = [0; 8];
    server.read_exact(&mut received).await?;
    assert_eq!(&received, b"verified");
    Ok(())
}

#[tokio::test]
async fn private_ca_succeeds_and_missing_trust_or_wrong_name_fails() {
    let identity = identity(None);
    exchange(&identity, Some(identity.root.as_bytes()), "service.example")
        .await
        .unwrap();
    let untrusted = exchange(&identity, None, "service.example")
        .await
        .unwrap_err();
    assert!(
        format!("{untrusted:?}").contains("UnknownIssuer"),
        "{untrusted:?}"
    );
    let wrong_name = exchange(&identity, Some(identity.root.as_bytes()), "wrong.example")
        .await
        .unwrap_err();
    assert!(
        format!("{wrong_name:?}").contains("NotValidForName"),
        "{wrong_name:?}"
    );
}

#[tokio::test]
async fn expired_and_future_leaf_certificates_fail_even_with_trusted_issuer() {
    for (validity, reason) in [((1999, 2000), "Expired"), ((4090, 4095), "NotValidYet")] {
        let identity = identity(Some(validity));
        let error = exchange(&identity, Some(identity.root.as_bytes()), "service.example")
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains(reason), "{error:?}");
    }
}

#[test]
fn malformed_trust_identity_and_key_mismatch_fail_locally() {
    let identity = identity(None);
    for trust in [
        &b""[..],
        b"not PEM",
        b"-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----\n",
    ] {
        assert!(verified_client_config(false, Some(trust)).is_err());
    }
    assert!(server_config(b"not PEM", identity.key.as_bytes()).is_err());
    assert!(server_config(identity.certificate.as_bytes(), b"not a key").is_err());
    let other_key = rcgen::KeyPair::generate().unwrap().serialize_pem();
    assert!(server_config(identity.certificate.as_bytes(), other_key.as_bytes()).is_err());
}

#[test]
fn server_name_keeps_ip_validation_and_explicit_name_override() {
    let ip = Host::Ip("127.0.0.1".parse().unwrap());
    assert!(matches!(
        server_name(&ip, None).unwrap(),
        ServerName::IpAddress(_)
    ));
    assert_eq!(
        server_name(&ip, Some("service.example")).unwrap(),
        ServerName::try_from("service.example").unwrap()
    );
    assert!(server_name(&Host::Wildcard, None).is_err());
    assert!(server_name(&ip, Some("invalid name")).is_err());
}
