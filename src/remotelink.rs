use log::{debug, trace, warn};
use std::sync::Arc;
use std::{io, mem};
use tokio::time::{error::Elapsed, Duration};
use tokio::{select, time};
use serde::{Deserialize, Serialize};

use crate::mqttbytes::v4::*;
use crate::mqttbytes::*;
use crate::network::Network;
use crate::rumqttlog::{
    Connection, ConnectionAck, Event, Notification, Receiver, RecvError, SendError, Sender, Disconnection,
};
use crate::state::{self, State};
use crate::{network, Id};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionLoginCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_client_id_len: usize,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub login_credentials: Option<Vec<ConnectionLoginCredentials>>,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

pub struct RemoteLink {
    max_inflight_count: u16,
    connect: Connect,
    id: Id,
    network: Network,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
    total: usize,
    pending: Vec<Notification>,
    pub(crate) state: State,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("State error")]
    State(#[from] state::Error),
    #[error("Unexpected router message")]
    RouterMessage(Notification),
    #[error("Connack error {0}")]
    ConnAck(String),
    #[error("Keep alive time exceeded")]
    KeepAlive,
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Payload count greater than max inflight")]
    TooManyPayloads(usize),
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Invalid username or password provided")]
    InvalidUsernameOrPassword,
    #[error("Disconnect request")]
    Disconnect,
}

impl RemoteLink {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(Id, Event)>,
        mut network: Network,
    ) -> Result<(String, Id, RemoteLink), Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let mut connect = time::timeout(timeout, async {
            let connect = network.read_connect().await?;

            network.client_id = connect.client_id.to_owned();

            // if let Some(l) = &connect.login {
            //     println!("A conexão chegou em {} com id '{}' e credenciais: {}/{}", network.addr, connect.client_id, l.username, l.password);
            // } else {
            //     println!("A conexão chegou em {} com id '{}' e sem credenciais", network.addr, connect.client_id);
            // }

            // Validate credentials if they exist
            if let Some(credentials) = &config.login_credentials {
                let validated = match &connect.login {
                    Some(l) => {
                        let mut validated = false;

                        // Iterate through all the potential credentials
                        for entry in credentials {
                            if l.validate(&entry.username, &entry.password) {
                                validated = true;
                                break;
                            }
                        }

                        validated
                    }
                    None => false,
                };

                // Return error if the username/password werenot found
                if !validated {
                    return Err(Error::InvalidUsernameOrPassword);
                }
            }

            Ok::<_, Error>(connect)
        })
        .await??;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let clean_session = connect.clean_session;
        if !clean_session && client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (mut connection, link_rx) = Connection::new_remote(&client_id, clean_session, 10);

        // Add last will to connection
        if let Some(will) = connect.last_will.take() {
            connection.set_will(will);
        }

        let message = (0, Event::Connect(connection, network.addr.to_owned()));
        router_tx.send(message).unwrap();

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        // Right now link identifies failure with dropped rx in router, which is probably ok for now
        let (id, session, pending) = match link_rx.recv()? {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success((id, session, pending)) => {
                    network.conn_id = id.to_string();
                    (id, session, pending)
                },
                ConnectionAck::Failure(reason) => return Err(Error::ConnAck(reason)),
            },
            message => return Err(Error::RouterMessage(message)),
        };

        // Send connection acknowledgement back to the client
        let connack = ConnAck::new(ConnectReturnCode::Success, session);
        network.connack(connack).await?;

        let max_inflight_count = config.max_inflight_count;
        Ok((
            client_id,
            id,
            RemoteLink {
                max_inflight_count,
                connect,
                id,
                network,
                router_tx,
                link_rx,
                total: 0,
                pending,
                state: State::new(max_inflight_count),
            },
        ))
    }

    /// A new network connection should wait for mqtt connect packet. This handling should be handled
    /// asynchronously to avoid listener from not blocking new connections while this connection is
    /// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
    /// denial of service attacks (rogue clients which only does network connection without sending
    /// mqtt connection packet to make make the server reach its concurrent connection limit)
    pub async fn new_connection(config: Arc<ConnectionSettings>, router_tx: Sender<(Id, Event)>, network: Network) -> Result<(), Error> {
        // let config = self.config.clone();
        // let router_tx = self.router_tx.clone();
        let addr = network.addr.to_owned();

        // Start the link
        let (client_id, id, mut link) = RemoteLink::new(config, router_tx.clone(), network).await?;
        println!("[{} {} {} ] New connection", addr, id, client_id);
        let (execute_will, pending) = match link.start().await {
            // Connection get close. This shouldn't usually happen
            Ok(_) => {
                println!("[{} {} {} ] Stopped!!", addr, id, client_id);
                (true, link.state.clean())
            }
            // We are representing clean close as Abort in `Network`
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::ConnectionAborted => {
                println!("[{} {} {} ] Closed!!", addr, id, client_id);
                (true, link.state.clean())
            }
            // Client requested disconnection.
            Err(Error::Disconnect) => {
                println!("[{} {} {} ] Disconnected!!", addr, id, client_id);
                (false, link.state.clean())
            }
            // Any other error
            Err(e) => {
                println!("[{} {} {} ] Error!! {}", addr, id, client_id, e.to_string());
                (true, link.state.clean())
            }
        };

        let disconnect = Disconnection::new(client_id, execute_will, pending);
        let disconnect = Event::Disconnect(disconnect);
        let message = (id, disconnect);
        router_tx.send(message)?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);
        let pending = mem::take(&mut self.pending);
        let mut pending = pending.into_iter();

        loop {
            select! {
                o = self.network.readb(&mut self.state) => {
                    let disconnect = o?;
                    self.handle_network_data().await?;

                    if disconnect {
                        return Err(Error::Disconnect)
                    }
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = async { pending.next() } => {
                    match message {
                        Some(message) => self.handle_router_response(message).await?,
                        None => break
                    };
                }
            }
        }

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.readb(&mut self.state) => {
                    let disconnect = o?;
                    self.handle_network_data().await?;

                    if disconnect {
                        return Err(Error::Disconnect)
                    }
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = self.link_rx.async_recv(), if !self.state.pause_outgoing() => {
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
            }
        }
    }

    async fn handle_network_data(&mut self) -> Result<(), Error> {
        let data = self.state.take_incoming();
        self.network.flush(self.state.write_mut()).await?;

        if !data.is_empty() {
            debug!(
                "{:11} {:14} Id = {}, Count = {}",
                "data",
                "remote",
                self.id,
                data.len()
            );

            let message = Event::Data(data);
            self.router_tx.send((self.id, message))?;
        }

        Ok(())
    }

    async fn handle_router_response(&mut self, message: Notification) -> Result<(), Error> {
        match message {
            Notification::ConnectionAck(_) => {}
            Notification::Acks(reply) => {
                debug!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "acks",
                    "reply",
                    self.id,
                    reply.len()
                );

                for ack in reply.into_iter() {
                    self.state.outgoing_ack(ack)?;
                }
            }
            Notification::Message(reply) => {
                let topic = reply.topic;
                let qos = qos(reply.qos).unwrap();
                let payload = reply.payload;

                println!(
                    "{:11} {:14} Id = {}, Topic = {}, Count = {}, Payload = '{}'",
                    "data",
                    "pending",
                    self.id,
                    topic,
                    payload.len(),
                    String::from_utf8_lossy(payload.as_ref()),
                );

                self.state.add_pending(topic, qos, vec![payload]);
                self.state.write_pending()?;
            }
            Notification::Data(reply) => {
                let topic = reply.topic;
                let qos = qos(reply.qos).unwrap();
                let payload = reply.payload;

                println!(
                    "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                    "data",
                    "reply",
                    self.id,
                    topic,
                    reply.cursor,
                    payload.len(),
                );

                let payload_count = payload.len();
                if payload_count > self.max_inflight_count as usize {
                    println!("ERR346 (payload_count > self.max_inflight_count) {}", self.network.addr);
                    return Err(Error::TooManyPayloads(payload_count));
                }

                self.total += payload_count;
                self.state.add_pending(topic, qos, payload);
                self.state.write_pending()?;
            }
            Notification::Pause => {
                let message = (self.id, Event::Ready);
                self.router_tx.send(message)?;
            }
            notification => {
                println!("{:?} not supported in remote link", notification);
            }
        }

        self.network.flush(self.state.write_mut()).await?;
        Ok(())
    }
}
