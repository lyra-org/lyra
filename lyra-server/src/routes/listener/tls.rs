// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The certificate the server presents when it serves HTTPS itself.

use std::{
    path::Path,
    sync::{
        Arc,
        PoisonError,
        RwLock,
    },
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};
use rustls::{
    ServerConfig,
    crypto::ring::default_provider,
    pki_types::{
        CertificateDer,
        PrivateKeyDer,
        pem::{
            self,
            PemObject,
        },
    },
    server::{
        ClientHello,
        ParsedCertificate,
        ResolvesServerCert,
    },
    sign::CertifiedKey,
};
use tokio_rustls::TlsAcceptor;

use crate::services::boot::TlsFiles;

/// The certificate and key in a pair of PEM files, served to each new connection and replaced
/// when the files are.
pub(crate) struct ServerTls {
    files: TlsFiles,
    certificate: Arc<CurrentCertificate>,
    config: Arc<ServerConfig>,
    serving: blake3::Hash,
    /// The last failure to reload, so a pair that stays broken is reported once.
    reload_failure: Option<String>,
}

#[derive(Debug)]
struct CurrentCertificate(RwLock<Arc<CertifiedKey>>);

impl ResolvesServerCert for CurrentCertificate {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(
            &self.0.read().unwrap_or_else(PoisonError::into_inner),
        ))
    }
}

impl ServerTls {
    /// Reads the pair, or says why it can't be served.
    pub(crate) async fn load(files: TlsFiles) -> Result<Self> {
        let pair = PemPair::read(&files).await?;
        let certificate = Arc::new(CurrentCertificate(RwLock::new(Arc::new(
            pair.certified(&files)?,
        ))));
        let mut config = ServerConfig::builder_with_provider(Arc::new(default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(&certificate) as _);
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            serving: pair.digest(),
            files,
            certificate,
            config: Arc::new(config),
            reload_failure: None,
        })
    }

    pub(crate) fn files(&self) -> &TlsFiles {
        &self.files
    }

    pub(super) fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(Arc::clone(&self.config))
    }

    /// Rereads the files every `interval`, for as long as it is polled.
    pub(super) async fn follow_files(mut self, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            self.reload().await;
        }
    }

    /// Serves the pair on disk to new connections when it has changed and works. One that doesn't
    /// leaves the previous certificate in place, as does a pair caught with only one file
    /// replaced, which the next reload finds whole.
    async fn reload(&mut self) {
        match self.replace_changed().await {
            Ok(replaced) => {
                self.reload_failure = None;
                if replaced {
                    tracing::info!(
                        cert = %self.files.cert.display(),
                        key = %self.files.key.display(),
                        "serving the replaced TLS certificate"
                    );
                }
            }
            Err(err) => {
                let failure = format!("{err:#}");
                if self.reload_failure.as_ref() != Some(&failure) {
                    tracing::warn!(
                        error = %failure,
                        "failed to reload the TLS certificate; still serving the previous one"
                    );
                    self.reload_failure = Some(failure);
                }
            }
        }
    }

    async fn replace_changed(&mut self) -> Result<bool> {
        let pair = PemPair::read(&self.files).await?;
        let digest = pair.digest();
        if digest == self.serving {
            return Ok(false);
        }
        let certified = Arc::new(pair.certified(&self.files)?);
        *self
            .certificate
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner) = certified;
        self.serving = digest;
        Ok(true)
    }
}

/// The contents of both files, read together.
struct PemPair {
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl PemPair {
    async fn read(files: &TlsFiles) -> Result<Self> {
        Ok(Self {
            cert: read(&files.cert, "certificate").await?,
            key: read(&files.key, "key").await?,
        })
    }

    fn digest(&self) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(self.cert.len() as u64).to_le_bytes());
        hasher.update(&self.cert);
        hasher.update(&self.key);
        hasher.finalize()
    }

    /// The pair as rustls serves it. Errors name the files and never quote them.
    fn certified(&self, files: &TlsFiles) -> Result<CertifiedKey> {
        let cert_path = files.cert.display();
        let key_path = files.key.display();
        let chain = CertificateDer::pem_slice_iter(&self.cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| anyhow!("TLS certificate at {cert_path} is not valid PEM"))?;
        if chain.is_empty() {
            bail!("TLS certificate at {cert_path} holds no PEM certificate");
        }
        // rustls checks only the first certificate against the key, and would serve the rest of
        // the chain unread.
        if chain
            .iter()
            .any(|certificate| ParsedCertificate::try_from(certificate).is_err())
        {
            bail!("TLS certificate at {cert_path} holds a certificate that can't be read");
        }
        if is_encrypted(&self.key) {
            bail!("TLS key at {key_path} is encrypted");
        }
        let key = PrivateKeyDer::from_pem_slice(&self.key).map_err(|err| match err {
            pem::Error::NoItemsFound => anyhow!("TLS key at {key_path} holds no PEM private key"),
            _ => anyhow!("TLS key at {key_path} is not valid PEM"),
        })?;
        CertifiedKey::from_der(chain, key, &default_provider()).map_err(|err| {
            anyhow!("TLS certificate at {cert_path} and key at {key_path} can't be served: {err}")
        })
    }
}

async fn read(path: &Path, what: &str) -> Result<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read the TLS {what} at {}", path.display()))
}

fn is_encrypted(key: &[u8]) -> bool {
    let contains = |marker: &[u8]| key.windows(marker.len()).any(|window| window == marker);
    contains(b"-----BEGIN ENCRYPTED PRIVATE KEY-----") || contains(b"Proc-Type: 4,ENCRYPTED")
}
