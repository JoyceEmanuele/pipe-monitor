use tokio::sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::{task, time};
use tokio::net::TcpListener;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::mqttbytes; // ::v4::Packet;
use crate::tcp_socket::{socket_forwarder, ConnectionAcceptor, DevBrokerSockets};
use crate::mqtt_io::{MqttPacketReader, ReadPacketResult};
use crate::tls_socket_rustls::create_rustls_config;
use crate::GlobalVars;

trait TokioRW: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Sized {}

pub struct ClientInfo {
	pub ident: Mutex<ClientInfoIdent>,
	pub addr: String,
	pub i_broker: usize,
	pub start: std::time::Instant,
	pub ended_client_w: AtomicBool,
	pub ended_server_w: AtomicBool,
}
pub struct ClientInfoIdent {
	pub dev_id: String,
	pub username: String,
	pub client_id: String,
}

pub fn run_server(globs: Arc<GlobalVars>) {
	let mut runtime = tokio::runtime::Builder::new_current_thread();
	let runtime = runtime.enable_all().build().expect("Não foi possível criar o runtime tokio");
	runtime.block_on(async {
		use futures::{stream::FuturesUnordered, StreamExt};
		let mut futures = FuturesUnordered::new();
		for i_broker in 0..globs.configfile.LISTEN_SOCKETS_DEVEL.len() {
			let socket = globs.configfile.LISTEN_SOCKETS_DEVEL[i_broker].to_owned();
			futures.push(tunnel_server_listener(socket, globs.clone(), i_broker));
		}
		let r = futures.next().await;
		println!("Stopping tunnel_server_listener. {:?}", r);
	});
}

async fn tunnel_server_listener(listen_socket: String, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(), String> {
	let listen: std::net::SocketAddr = listen_socket.parse().expect("SocketAddr inválido");
	let delay = Duration::from_millis(1);

	let listener = TcpListener::bind(&listen).await.map_err(|err| format!("ERR50 {}", err))?;

	println!("Waiting for connections on {}", listen);
	loop {
		// Await new network connection.
		let (stream, addr) = match listener.accept().await {
			Ok((s, r)) => (s, r),
			Err(err) => {
				println!("Unable to accept socket: {:?}", err);
				continue;
			}
		};
		let addr = addr.to_string();

		let (tls_config, tls_type) = {
			let i_tls = (rand::random::<f64>() * 0.).floor() as isize;
			if i_tls == 0 { (ConnectionAcceptor::Rustls, "Rustls") }
			else { (ConnectionAcceptor::Direct, "Direct") }
		};

		if globs.enable_debug.load(Ordering::Relaxed) == true {
			let broker_addr = &globs.configfile.BROKERS[i_broker];
			println!("[{} {}] Chegou conexão TCP. Direcionando para {} usando {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, broker_addr.host, tls_type);
		}

		let globs = globs.clone();
		task::spawn(async move {
			let dev = Arc::new(ClientInfo{
				ident: Mutex::new(ClientInfoIdent{
					dev_id: "".to_owned(),
					username: "".to_owned(),
					client_id: "".to_owned(),
				}),
				addr: addr.to_owned(),
				i_broker,
				start: std::time::Instant::now(),
				ended_client_w: AtomicBool::new(false),
				ended_server_w: AtomicBool::new(false),
			});

			let r = socket_forwarder1(tls_config, stream, dev.clone(), globs.clone(), i_broker).await;

			let duration = dev.start.elapsed();

			let (client_id, dev_id) = {
				let ident = dev.ident.lock().await;
				(ident.client_id.clone(), ident.dev_id.clone())
			};
			if client_id.len() > 0 {
				match r {
					Ok((r1, r2)) => println!("[{} {}  {}/{}] Conexão finalizada após {:?} ms: {:?} {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, client_id, dev_id, duration.as_millis(), r1, r2),
					Err(err) =>     println!("[{} {}  {}/{}] Conexão finalizada após {:?} ms: {}",        &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, client_id, dev_id, duration.as_millis(), err),
				};
			}
		});

		time::sleep(delay).await;
	}
}

async fn socket_forwarder1(tls_config: ConnectionAcceptor, device_stream: tokio::net::TcpStream, dev: Arc<ClientInfo>, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(Result<(), String>, Result<(), String>), String> {
	let pair: DevBrokerSockets = socket_forwarder(tls_config, device_stream, &dev.addr, globs.clone(), i_broker).await?;
	return match pair {
		DevBrokerSockets::Direct(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, true, globs, i_broker).await,
		DevBrokerSockets::Rustls(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, false, globs, i_broker).await,
		DevBrokerSockets::Rustls2Tcp(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, false, globs, i_broker).await,
	};
}

async fn socket_forwarder2<T1: AsyncRead+AsyncWrite, T2: AsyncRead+AsyncWrite>(device_stream: T1, broker_stream: T2, dev1: Arc<ClientInfo>, direct: bool, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(Result<(), String>, Result<(), String>), String> {
	let (mut ri, mut wi) = tokio::io::split(device_stream);
	let (mut ro, mut wo) = tokio::io::split(broker_stream);

	let dev2 = dev1.clone();
	let globs_clone = globs.clone();

	let client_to_server_fut = async {
		let r1 = if direct {
			// tokio::io::copy(&mut ri, &mut wo).await?;
			transfer_direct(&mut ri, &mut wo, dev1.clone(), "dv->br").await
		} else {
			transfer_with_sniffing(&mut ri, &mut wo, dev1.clone(), "dv->br", globs_clone, i_broker).await
		};
		match wo.shutdown().await {
			Ok(()) => { dev1.ended_server_w.store(true, Ordering::Relaxed); },
			Err(err) => { println!("Não foi possível dar shutdown no socket de escrita para o broker: {}", err); },
		};
		r1
	};

	let server_to_client_fut = async {
		let r2 = if direct {
			// tokio::io::copy(&mut ro, &mut wi).await?;
			transfer_direct(&mut ro, &mut wi, dev2.clone(), "dv<-br").await
		} else {
			transfer_with_sniffing(&mut ro, &mut wi, dev2.clone(), "dv<-br", globs, i_broker).await
		};
		match wi.shutdown().await {
			Ok(()) => { dev2.ended_client_w.store(true, Ordering::Relaxed); },
			Err(err) => { println!("Não foi possível dar shutdown no socket de escrita para o device: {}", err); },
		};
		r2
	};

	// tokio::try_join!(client_to_server, server_to_client).map_err(|e| format!("ERR367 {}", e))?;
	let (r1, r2) = tokio::join!(
		client_to_server_fut,
		server_to_client_fut,
	);

	Ok((r1, r2))
}

async fn transfer_direct<T1: AsyncRead+Unpin, T2: AsyncWrite+Unpin>(r: &mut T1, w: &mut T2, dev: Arc<ClientInfo>, direction: &str) -> Result<(), String> {
	// let direction = "device"; // to broker
	let mut buffer_in = bytes::BytesMut::with_capacity(10 * 1024);
	loop {
		let read = r.read_buf(&mut buffer_in).await.map_err(|err| format!("Não foi possível ler do {}: {}", direction, err))?;
		if read != 0 {
			w.write_all(&buffer_in[(buffer_in.len()-read)..buffer_in.len()]).await.map_err(|err| format!("Não foi possível encaminhar o que veio do {}: {}", direction, err))?;
		}
		if read == 0 {
			buffer_in.clear();
			let ident = dev.ident.lock().await;
			println!("[{} {} {} {}] Erro ao tentar ler do {} - Conexão encerrada", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction);
			return Ok(());
		}
		buffer_in.clear();
	}
}

async fn transfer_with_sniffing<T1: AsyncRead+Unpin, T2: AsyncWrite+Unpin>(r: &mut T1, w: &mut T2, dev: Arc<ClientInfo>, direction: &str, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(), String> {
	// let direction = "device"; // to broker
	let mut mqtt_reader = MqttPacketReader::new();

	// if direction == "dv->br" {
	// 	println!("Tem 30 segundos para mandar primeiro pacote MQTT");
	// 	let r = tokio::time::timeout(
	// 		Duration::from_secs(30),
	// 		mqtt_reader.read_packet(r, w)
	// 	)
	// 		.await
	// 		.map_err(|_e| "Tempo esgotado esperando o pacote de Connect".to_owned())
	// 		.and_then(|v| v);

	// 	let packet = match r {
	// 		Ok(v) => {
	// 			println!("Primeiro pacote MQTT interpretado");
	// 			v
	// 		},
	// 		Err(err) => {
	// 			let dev = dev.lock().map_err(|err| format!("Erro ao pegar lock do dev: {}", err))?;
	// 			println!("[{} {} {} {}] Finalizando sniffing do que chega do {}: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, dev.client_id, direction, err);
	// 			return Err(format!("Finalizando sniffing do que chega do {}: {}", direction, err));
	// 		},
	// 	};
	// 	let mut dev = dev.lock().map_err(|err| format!("Erro ao pegar lock do dev: {}", err))?;
	// 	analize_packet(packet, &mut dev, &direction);
	// }

	let mut max_packet_size = 5_000;
	let mut ignore_mqtt_errors = false;
	loop {
		let (packet, negado) = match mqtt_reader.read_packet(r, max_packet_size, 30 * 60).await {
			Ok(packet) => {
				let mut ident = dev.ident.lock().await;
				// let packet = analize_packet(packet, &mut dev, &direction);

				let mut negado = false;

				match &packet {
					mqttbytes::v4::Packet::Connect(p) => {
						ident.client_id = p.client_id.to_owned();
						if let Some(lw) = &p.last_will {
							if lw.message.starts_with(b"DC ") {
								if let Ok(dev_id) = std::str::from_utf8(&lw.message[3..]) {
									ident.dev_id = dev_id.to_owned();
								}
							}
						}
						println!("[{} {} {} {}/{}] {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, ident.dev_id, packet);
						ident.username = p.login.as_ref().and_then(|v| Some(&v.username[..])).unwrap_or("").to_owned();

						if ident.username == "develuser" { /* OK */ }
						else if ident.username == "develuser2" { /* OK */ }
						else {
							return Err(format!("Usuário não permitido neste gateway: {}", ident.username));
						}
					},
					// mqttbytes::v4::Packet::ConnAck(_p) => {
					// 	// O ideal era contabilizar a conexão MQTT só aqui no ConnAck com sucesso, mas como o dev.client_id é inicializado no Connect,
					// 	// se tiver desconexão depois de salvar o dev.client_id ela vai entrar nas estatísticas de desconexão. Então a estatística de
					// 	// conexão tem que ser feita junto com a inicialização do dev.client_id.
					// },
					mqttbytes::v4::Packet::Subscribe(p) => {
						for filter in &p.filters {
							if globs.enable_debug.load(Ordering::Relaxed) == true {
								println!("[{} {} {} {}] Subscribe '{}'", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, filter.path);
							}
						}
					},
					// dashserver, dacr2, devsv3, devkhomp, devkron
					mqttbytes::v4::Packet::Publish(p) => {
						if p.payload.len() > 5000 {
							match std::str::from_utf8(&p.payload) {
								Ok(payload) => { println!("[{} {} {} {}] Payload muito grande ({}) no tópico {}: {}...", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, p.payload.len(), p.topic, &payload[..500]); },
								Err(_err) => { println!("[{} {} {} {}] Payload muito grande ({}) no tópico {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, p.payload.len(), p.topic); },
							}
						}
						// println!("[{} {} {} {}] Publish '{}' => '{}'", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, dev.client_id, p.topic, String::from_utf8_lossy(p.payload.as_ref()));
					},
					mqttbytes::v4::Packet::PingReq => {},
					mqttbytes::v4::Packet::PingResp => {},
					mqttbytes::v4::Packet::PubAck(_p) => {},
					// mqttbytes::v4::Packet::PubRec(_p) => { println!("[{}:{}] {:?}", self.id, self.client_id, packet); },
					// mqttbytes::v4::Packet::PubRel(_p) => { println!("[{}:{}] {:?}", self.id, self.client_id, packet); },
					// mqttbytes::v4::Packet::PubComp(_p) => { println!("[{}:{}] {:?}", self.id, self.client_id, packet); },
					// mqttbytes::v4::Packet::SubAck(_p) => {},
					// mqttbytes::v4::Packet::Unsubscribe(_p) => { println!("[{}:{}] {:?}", self.id, self.client_id, packet); },
					// mqttbytes::v4::Packet::UnsubAck(_p) => {},
					// mqttbytes::v4::Packet::Disconnect => { println!("[{}:{}] {:?}", self.id, self.client_id, packet); },
					_packet => {
						if globs.enable_debug.load(Ordering::Relaxed) == true {
							println!("[{} {} {} {}/{}] {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, ident.dev_id, _packet);
						}
					},
				}

				(packet, negado)
			},
			Err(ReadPacketResult::SocketError(err)) => {
				let ident = dev.ident.lock().await;
				println!("[{} {} {} {}] Finalizando sniffing do que chega do {}: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction, err);
				return Err(format!("Finalizando sniffing do que chega do {}: {}", direction, err));
			},
			Err(ReadPacketResult::MqttError(err)) => {
				if ignore_mqtt_errors {}
				else {
					let ident = dev.ident.lock().await;
					println!("[{} {} {} {}] Interrompendo sniffing do que chega do {}: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction, err);
					ignore_mqtt_errors = true;
				}
				return Err(format!("L42 - Finalizando sniffing do que chega do {}: {}", direction, err));
			},
			Err(ReadPacketResult::Timeout) => {
				let err = "Timeout tentando ler do socket o próximo pacote MQTT".to_owned();
				let ident = dev.ident.lock().await;
				println!("[{} {} {} {}] Finalizando sniffing do que chega do {}: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction, err);
				return Err(format!("Finalizando sniffing do que chega do {}: {}", direction, err));
			},
		};
		if !negado {
			mqtt_reader.write_packet(w, packet).await?;
		}
	}
}
