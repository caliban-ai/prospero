//! Network-agnostic transport seam for the caliband protocol (ADR 0051).
//!
//! Turns an [`Endpoint`] (+ optional TLS + optional bearer token) into a duplex
//! byte stream. The NDJSON protocol rides on top of a [`BoxConn`] unchanged —
//! TLS and the token preamble are framing below it. Ported from
//! `caliban-supervisor::transport`; prospero needs the client [`connect`] path
//! in production, and the server [`Listener`] path only for `FakeCaliband`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::net::UnixStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::caliband::wire::Endpoint;

/// A duplex byte stream over any transport family.
pub trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

/// Boxed duplex connection handed to the NDJSON protocol layer.
pub type BoxConn = Box<dyn Conn>;

/// Client-side TLS material.
#[derive(Clone)]
pub struct TlsClient {
    /// Handshake connector built from a trusted CA store.
    pub connector: TlsConnector,
    /// Expected server name (SNI / cert validation target).
    pub server_name: String,
}

/// Install the `ring` crypto provider as the process default, exactly once.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build client TLS trusting `ca_pem`, verifying the server presents `server_name`.
pub fn tls_client_from_pem(ca_pem: &[u8], server_name: &str) -> std::io::Result<TlsClient> {
    ensure_crypto_provider();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        roots
            .add(cert.map_err(|e| std::io::Error::other(e.to_string()))?)
            .map_err(std::io::Error::other)?;
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsClient {
        connector: TlsConnector::from(Arc::new(config)),
        server_name: server_name.to_string(),
    })
}

/// Bearer-token preamble: `{"bearer":"<token>"}\n`, sent after the TLS
/// handshake so it travels encrypted. TCP only; Unix never sends it.
#[derive(Serialize, Deserialize)]
struct TokenPreamble {
    bearer: String,
}

async fn client_send_token(conn: &mut BoxConn, token: &str) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&TokenPreamble {
        bearer: token.to_string(),
    })
    .map_err(std::io::Error::other)?;
    line.push(b'\n');
    conn.write_all(&line).await?;
    conn.flush().await
}

/// How to dial a connection.
pub struct ConnectSpec {
    /// Target address.
    pub endpoint: Endpoint,
    /// TLS (TCP only).
    pub tls: Option<TlsClient>,
    /// Bearer token to present (TCP only).
    pub token: Option<String>,
}

/// Dial a connection per `spec`: TLS handshake when configured, then the
/// bearer-token preamble when a token is configured.
pub async fn connect(spec: &ConnectSpec) -> std::io::Result<BoxConn> {
    match &spec.endpoint {
        Endpoint::Unix { path } => Ok(Box::new(UnixStream::connect(path).await?)),
        Endpoint::Tcp { addr } => {
            let stream = TcpStream::connect(addr).await?;
            let mut conn: BoxConn = match &spec.tls {
                None => Box::new(stream),
                Some(t) => {
                    let name = ServerName::try_from(t.server_name.clone())
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                    Box::new(t.connector.connect(name, stream).await?)
                }
            };
            if let Some(token) = &spec.token {
                client_send_token(&mut conn, token).await?;
            }
            Ok(conn)
        }
    }
}

// ---- Server half: only compiled for the test harness (`FakeCaliband`). ----
#[cfg(any(test, feature = "testkit"))]
mod server {
    use super::{BoxConn, Endpoint, TokenPreamble, ensure_crypto_provider};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt as _;
    use tokio::net::{TcpListener, UnixListener};
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

    /// Server-side TLS material.
    #[derive(Clone)]
    pub struct TlsServer {
        /// Handshake acceptor built from a cert chain + private key.
        pub acceptor: TlsAcceptor,
    }

    /// Build server TLS from a PEM cert chain + private key.
    pub fn tls_server_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> std::io::Result<TlsServer> {
        ensure_crypto_provider();
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<_, _>>()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let key: PrivateKeyDer<'static> = PrivateKeyDer::from_pem_slice(key_pem)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(std::io::Error::other)?;
        Ok(TlsServer {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    async fn read_preamble_line(conn: &mut BoxConn) -> std::io::Result<String> {
        let mut buf = Vec::with_capacity(128);
        let mut byte = [0u8; 1];
        loop {
            let n = conn.read(&mut byte).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "no token preamble",
                ));
            }
            if byte[0] == b'\n' {
                break;
            }
            buf.push(byte[0]);
            if buf.len() > 4096 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "token preamble too long",
                ));
            }
        }
        String::from_utf8(buf).map_err(std::io::Error::other)
    }

    async fn server_check_token(conn: &mut BoxConn, expected: &str) -> std::io::Result<()> {
        let line = read_preamble_line(conn).await?;
        let preamble: TokenPreamble = serde_json::from_str(&line).map_err(std::io::Error::other)?;
        if preamble.bearer == expected {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "bad bearer token",
            ))
        }
    }

    /// How to bind a listener.
    pub struct BindSpec {
        /// Address family + address.
        pub endpoint: Endpoint,
        /// TLS (TCP only). `None` = plaintext.
        pub tls: Option<TlsServer>,
        /// Required bearer token for network connections.
        pub token: Option<String>,
    }

    /// A bound listener over one transport family.
    pub enum Listener {
        /// Unix-domain.
        Unix(UnixListener),
        /// TCP (TLS/token applied at accept-time).
        Tcp {
            /// Underlying listener.
            listener: TcpListener,
            /// Server TLS material, if any.
            tls: Option<TlsServer>,
            /// Required bearer token, if any.
            token: Option<String>,
        },
    }

    impl Listener {
        /// Bind a listener per `spec`.
        pub async fn bind(spec: &BindSpec) -> std::io::Result<Listener> {
            match &spec.endpoint {
                Endpoint::Unix { path } => {
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let _ = tokio::fs::remove_file(path).await;
                    Ok(Listener::Unix(UnixListener::bind(path)?))
                }
                Endpoint::Tcp { addr } => Ok(Listener::Tcp {
                    listener: TcpListener::bind(addr).await?,
                    tls: spec.tls.clone(),
                    token: spec.token.clone(),
                }),
            }
        }

        /// The actually-bound TCP address (resolves `:0`); `None` for Unix.
        pub fn local_addr(&self) -> Option<String> {
            match self {
                Listener::Unix(_) => None,
                Listener::Tcp { listener, .. } => listener.local_addr().ok().map(|a| a.to_string()),
            }
        }

        /// Accept one connection, performing the TLS handshake + token check
        /// (TCP) when configured.
        pub async fn accept(&self) -> std::io::Result<BoxConn> {
            match self {
                Listener::Unix(l) => {
                    let (stream, _addr) = l.accept().await?;
                    Ok(Box::new(stream))
                }
                Listener::Tcp {
                    listener,
                    tls,
                    token,
                } => {
                    let (stream, _addr) = listener.accept().await?;
                    let mut conn: BoxConn = match tls {
                        None => Box::new(stream),
                        Some(t) => Box::new(t.acceptor.accept(stream).await?),
                    };
                    if let Some(expected) = token {
                        server_check_token(&mut conn, expected).await?;
                    }
                    Ok(conn)
                }
            }
        }
    }
}

#[cfg(any(test, feature = "testkit"))]
pub use server::{BindSpec, Listener, TlsServer, tls_server_from_pem};

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    async fn echo_once(listener: Listener) {
        let mut c = listener.accept().await.expect("accept");
        let mut buf = [0u8; 5];
        c.read_exact(&mut buf).await.expect("read");
        c.write_all(&buf).await.expect("write");
        c.flush().await.expect("flush");
    }

    #[tokio::test]
    async fn tcp_tls_token_round_trip() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();

        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Tcp {
                addr: "127.0.0.1:0".into(),
            },
            tls: Some(tls_server_from_pem(&cert_pem, &key_pem).unwrap()),
            token: Some("s3cr3t".into()),
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(echo_once(listener));

        let mut c = connect(&ConnectSpec {
            endpoint: Endpoint::Tcp { addr },
            tls: Some(tls_client_from_pem(&cert_pem, "localhost").unwrap()),
            token: Some("s3cr3t".into()),
        })
        .await
        .unwrap();
        c.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bad_token_is_rejected() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();
        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Tcp {
                addr: "127.0.0.1:0".into(),
            },
            tls: Some(tls_server_from_pem(&cert_pem, &key_pem).unwrap()),
            token: Some("right".into()),
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let r = connect(&ConnectSpec {
            endpoint: Endpoint::Tcp { addr },
            tls: Some(tls_client_from_pem(&cert_pem, "localhost").unwrap()),
            token: Some("wrong".into()),
        })
        .await;
        // connect() sends the token then returns Ok; the server rejects on
        // accept and drops the connection, so a subsequent read sees EOF/err.
        if let Ok(mut c) = r {
            let mut b = [0u8; 1];
            assert!(c.read(&mut b).await.map(|n| n == 0).unwrap_or(true));
        }
    }

    #[tokio::test]
    async fn unix_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sock");
        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Unix { path: path.clone() },
            tls: None,
            token: None,
        })
        .await
        .unwrap();
        let server = tokio::spawn(echo_once(listener));
        let mut c = connect(&ConnectSpec {
            endpoint: Endpoint::Unix { path },
            tls: None,
            token: None,
        })
        .await
        .unwrap();
        c.write_all(b"world").await.unwrap();
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"world");
        server.await.unwrap();
    }
}
