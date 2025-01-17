use std::sync::Arc;
use std::time::Duration;
use crate::GlobalVars;
use tokio::net::TcpStream;
use crate::tls_socket_rustls;

#[derive(Debug)]
pub enum ConnectionAcceptor {
	Direct,
	Rustls,
}

pub enum DevBrokerSockets {
	Direct(tokio::net::TcpStream, tokio::net::TcpStream),
	Rustls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>, tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
	Rustls2Tcp(tokio_rustls::TlsStream<tokio::net::TcpStream>, tokio::net::TcpStream),
}

#[derive(Debug)]
pub enum SocketType {
	Direct(tokio::net::TcpStream),
	Rustls(tokio_rustls::TlsStream<tokio::net::TcpStream>),
}

pub async fn socket_forwarder(tls_config: ConnectionAcceptor, device_stream: tokio::net::TcpStream, dev_addr: &str, globs: Arc<GlobalVars>, i_broker: usize) -> Result<DevBrokerSockets, String> {
	let broker_addr = (
		globs.configfile.BROKERS[i_broker].host.to_owned(),
		globs.configfile.BROKERS[i_broker].port,
	);
	let incoming_socket = tokio::time::timeout(
		Duration::from_secs(30),
		stablish_incoming_socket(tls_config, device_stream, dev_addr, &globs)
	)
		.await
		.map_err(|_e| "TCP to TLS operation timed out".to_owned())
		.and_then(|v| v);
	let incoming_socket = match incoming_socket {
		Ok(v) => v,
		Err(err) => {
			return Err(err);
		}
	};

	let outgoing_socket = tokio::time::timeout(
		Duration::from_secs(30),
		stablish_outgoing_socket(broker_addr)
	)
		.await
		.map_err(|_e| "Connect to broker operation timed out".to_owned())
		.and_then(|v| v);
	let outgoing_socket = match outgoing_socket {
		Ok(v) => v,
		Err(err) => {
			return Err(err);
		}
	};

	match (incoming_socket, outgoing_socket) {
		(SocketType::Rustls(incoming_socket), SocketType::Direct(outgoing_socket)) => {
			return Ok(DevBrokerSockets::Rustls2Tcp(incoming_socket, outgoing_socket));
		},
		(SocketType::Direct(incoming_socket), SocketType::Direct(outgoing_socket)) => {
			return Ok(DevBrokerSockets::Direct(incoming_socket, outgoing_socket));
		},
		(a, b) => {
			return Err(format!("ERR251 - Tipos de socket não suportado: {:?} / {:?}", a, b));
		},
	};
}

async fn stablish_incoming_socket(tls_config: ConnectionAcceptor, device_stream: tokio::net::TcpStream, dev_addr: &str, globs: &Arc<GlobalVars>) -> Result<SocketType, String> {
	match tls_config {
		ConnectionAcceptor::Direct => {
			return Ok(SocketType::Direct(device_stream));
		},

		ConnectionAcceptor::Rustls => {
			let device_stream = tls_socket_rustls::stablish_incoming_socket(
				device_stream,
				dev_addr,
				globs,
			).await?;
			return Ok(SocketType::Rustls(device_stream));
		},
	};
}

async fn stablish_outgoing_socket(broker_host_port: (String, u16)) -> Result<SocketType, String> {
	let broker_addr = format!("{}:{}", broker_host_port.0, broker_host_port.1);
	let broker_stream = TcpStream::connect(broker_addr).await.map_err(|e| format!("ERR303 {}", e))?;
	return Ok(SocketType::Direct(broker_stream));
}
