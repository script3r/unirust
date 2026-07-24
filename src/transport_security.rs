use std::fs;
use std::path::Path;

use anyhow::Result;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

fn read_nonempty(path: &Path, description: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        anyhow::bail!("{description} {} is empty", path.display());
    }
    Ok(bytes)
}

/// Load a server identity and require client certificates signed by `client_ca`.
pub fn load_server_mtls(
    cert: Option<&Path>,
    key: Option<&Path>,
    client_ca: Option<&Path>,
) -> Result<Option<ServerTlsConfig>> {
    match (cert, key, client_ca) {
        (None, None, None) => Ok(None),
        (Some(cert), Some(key), Some(client_ca)) => {
            let cert = read_nonempty(cert, "server certificate")?;
            let key = read_nonempty(key, "server private key")?;
            let client_ca = read_nonempty(client_ca, "client CA certificate")?;
            Ok(Some(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(cert, key))
                    .client_ca_root(Certificate::from_pem(client_ca)),
            ))
        }
        _ => anyhow::bail!(
            "mTLS server configuration requires certificate, private key, and client CA together"
        ),
    }
}

/// Load a client identity and an explicit CA for server verification.
pub fn load_client_mtls(
    ca: Option<&Path>,
    cert: Option<&Path>,
    key: Option<&Path>,
) -> Result<Option<ClientTlsConfig>> {
    match (ca, cert, key) {
        (None, None, None) => Ok(None),
        (Some(ca), Some(cert), Some(key)) => {
            let ca = read_nonempty(ca, "server CA certificate")?;
            let cert = read_nonempty(cert, "client certificate")?;
            let key = read_nonempty(key, "client private key")?;
            Ok(Some(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .identity(Identity::from_pem(cert, key)),
            ))
        }
        _ => anyhow::bail!(
            "mTLS client configuration requires CA, certificate, and private key together"
        ),
    }
}
