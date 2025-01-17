use jackiechan::Sender;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use tokio::net::TcpListener;
use tokio::{task, time};

use rustls_pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::{
    server::AllowAnyAuthenticatedClient, Certificate, PrivateKey,
};

use std::fs::File;
use std::io::BufReader;

use crate::rumqttlog::Event;
use crate::network::Network;
use crate::Error::ServerKeyNotFound;
use crate::Id;
use crate::Error;
use crate::remotelink::{RemoteLink, ConnectionSettings};

enum ServerTLSAcceptor {
    RustlsAcceptor { acceptor: tokio_rustls::TlsAcceptor },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ServerCert {
    RustlsCert {
        ca_path: String,
        cert_path: String,
        key_path: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub listen: SocketAddr,
    pub cert: Option<ServerCert>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

pub struct Server {
    id: String,
    config: ServerSettings,
    router_tx: Sender<(Id, Event)>,
}

impl Server {
    pub fn new(id: String, config: ServerSettings, router_tx: Sender<(Id, Event)>) -> Server {
        Server {
            id,
            config,
            router_tx,
        }
    }

    fn tls_rustls(
        &self,
        cert_path: &str,
        key_path: &str,
        ca_path: &str,
    ) -> Result<Option<ServerTLSAcceptor>, Error> {
        use tokio_rustls::rustls::{RootCertStore, ServerConfig};

        let (certs, key) = {
            // Get certificates
            let cert_file = File::open(&cert_path);
            let cert_file =
                cert_file.map_err(|_| Error::ServerCertNotFound(cert_path.to_owned()))?;
            let certs = certs(&mut BufReader::new(cert_file));
            let certs = certs.map_err(|_| Error::InvalidServerCert(cert_path.to_string()))?;
            let certs = certs
                .iter()
                .map(|cert| Certificate(cert.to_owned()))
                .collect();

            // Get private key
            let key_file = File::open(&key_path);
            let key_file = key_file.map_err(|_| ServerKeyNotFound(key_path.to_owned()))?;
            let keys = rsa_private_keys(&mut BufReader::new(key_file));
            let keys = keys.map_err(|_| Error::InvalidServerKey(key_path.to_owned()))?;

            // Get the first key
            let key = match keys.first() {
                Some(k) => k.clone(),
                None => return Err(Error::InvalidServerKey(key_path.to_owned())),
            };

            (certs, PrivateKey(key))
        };

        // client authentication with a CA. CA isn't required otherwise
        let server_config = {
            let ca_file = File::open(ca_path);
            let ca_file = ca_file.map_err(|_| Error::CaFileNotFound(ca_path.to_owned()))?;
            let ca_file = &mut BufReader::new(ca_file);
            let ca_certs = rustls_pemfile::certs(ca_file)?;
            let ca_cert = ca_certs
                .first()
                .map(|c| Certificate(c.to_owned()))
                .ok_or_else(|| Error::InvalidCACert(ca_path.to_string()))?;
            let mut store = RootCertStore::empty();
            store
                .add(&ca_cert)
                .map_err(|_| Error::InvalidCACert(ca_path.to_string()))?;
            ServerConfig::builder()
                .with_safe_defaults()
                .with_client_cert_verifier(AllowAnyAuthenticatedClient::new(store))
                .with_single_cert(certs, key)?
        };

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        Ok(Some(ServerTLSAcceptor::RustlsAcceptor { acceptor }))
    }

    pub async fn start(&self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count = 0;

        let config = Arc::new(self.config.connections.clone());

        // Get the ServerTLSAcceptor which allow us to use either Rustls or Native TLS
        let acceptor = match &self.config.cert {
            Some(c) => match c {
                ServerCert::RustlsCert {
                    ca_path,
                    cert_path,
                    key_path,
                } => self.tls_rustls(cert_path, key_path, ca_path)?,
            },
            None => None,
        };

        let max_incoming_size = config.max_payload_size;

        println!(
            "Waiting for connections on {}. Server = {}",
            self.config.listen, self.id
        );
        loop {
            // Await new network connection.
            let (stream, addr) = match listener.accept().await {
                Ok((s, r)) => (s, r),
                Err(_e) => {
                    println!("Unable to accept socket.");
                    continue;
                }
            };

            // Depending on TLS or not create a new Network
            let network = match &acceptor {
                Some(a) => {
                    println!("{}. Accepting TLS connection from: {}", count, addr);

                    // Depending on which acceptor we're using address accordingly..
                    match a {
                        ServerTLSAcceptor::RustlsAcceptor { acceptor } => {
                            let stream = match acceptor.accept(stream).await {
                                Ok(v) => v,
                                Err(e) => {
                                    println!("Failed to accept TLS connection using Rustls. Error = {:?}", e);
                                    continue;
                                }
                            };

                            Network::new(stream, max_incoming_size, addr.to_string())
                        }
                    }
                }
                None => {
                    println!("{}. Accepting TCP connection from: {}", count, addr);
                    Network::new(stream, max_incoming_size, addr.to_string())
                }
            };

            count += 1;

            let config = config.clone();
            let router_tx = self.router_tx.clone();

            // Spawn a new thread to handle this connection.
            task::spawn(async {
                if let Err(e) = RemoteLink::new_connection(config, router_tx, network).await {
                    println!("Dropping link task!! Result = {:?}", e);
                }
            });

            time::sleep(delay).await;
        }
    }
}
