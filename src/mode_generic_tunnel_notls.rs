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
		let socket = &globs.configfile.LISTEN_SOCKET;
		let r = tunnel_server_listener(&socket, globs.clone()).await;
		println!("Stopping tunnel_server_listener. {:?}", r);
	});
}

async fn tunnel_server_listener(listen_socket: &str, globs: Arc<GlobalVars>) -> Result<(), String> {
	let listen: std::net::SocketAddr = listen_socket.parse().expect("SocketAddr inválido");
	let delay = Duration::from_millis(1);

	let listener = TcpListener::bind(&listen).await.map_err(|err| format!("ERR50 {}", err))?;

	let mut i_broker = globs.configfile.BROKERS.len();

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

		i_broker += 1;
		if i_broker >= globs.configfile.BROKERS.len() {
			i_broker = 0;
		}

		let (tls_config, tls_type) = (ConnectionAcceptor::Direct, "Direct");

		if globs.enable_debug.load(Ordering::Relaxed) == true {
			let broker_addr = &globs.configfile.BROKERS[i_broker];
			println!("[{} {}] Chegou conexão TCP. Direcionando para {} usando {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, broker_addr.host, tls_type);
		}

		task::spawn(
			on_device_connected(tls_config, stream, globs.clone(), i_broker, addr)
		);

		time::sleep(delay).await;
	}
}

async fn on_device_connected(tls_config: ConnectionAcceptor, device_stream: tokio::net::TcpStream, globs: Arc<GlobalVars>, i_broker: usize, addr: String) {
	globs.stats_counters.b_stats[i_broker].conn_arr.fetch_add(1, Ordering::Relaxed);
	globs.stats_counters.b_stats[i_broker].est_conn.fetch_add(1, Ordering::Relaxed);
	globs.stats_counters.est_conn.fetch_add(1, Ordering::Relaxed);

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

	let r = socket_forwarder1(tls_config, device_stream, dev.clone(), globs.clone(), i_broker).await;

	let duration = dev.start.elapsed();

	globs.stats_counters.b_stats[i_broker].conn_closed.fetch_add(1, Ordering::Relaxed);
	globs.stats_counters.b_stats[i_broker].est_conn.fetch_sub(1, Ordering::Relaxed);
	globs.stats_counters.est_conn.fetch_sub(1, Ordering::Relaxed);

	let (client_id, dev_id) = {
		let ident = dev.ident.lock().await;
		(ident.client_id.clone(), ident.dev_id.clone())
	};
	if client_id.len() > 0 || client_id.len() == 0 {
		match r {
			Ok((r1, r2)) => println!("[{} {}  {}/{}] Conexão finalizada após {:?} ms: {:?} {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, client_id, dev_id, duration.as_millis(), r1, r2),
			Err(err) =>     println!("[{} {}  {}/{}] Conexão finalizada após {:?} ms: {}",        &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), addr, client_id, dev_id, duration.as_millis(), err),
		};
	}

	let dev_id = if dev_id.len() > 0 {
		dev_id
	} else {
		client_id
	};
	if !dev_id.is_empty() {
		let mut devs_conn = globs.stats_counters.devs_conn.lock().await;
		match devs_conn.get_mut(&dev_id) {
			Some(counter) => {
				*counter -= 1;
			},
			None => {
				devs_conn.insert(dev_id, -1);
			}
		};
	}
}

async fn socket_forwarder1(tls_config: ConnectionAcceptor, device_stream: tokio::net::TcpStream, dev: Arc<ClientInfo>, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(Result<(), String>, Result<(), String>), String> {
	println!("DBG138: {:?}", tls_config);
	let pair: DevBrokerSockets = match socket_forwarder(tls_config, device_stream, &dev.addr, globs.clone(), i_broker).await {
		Ok(v) => v,
		Err(err) => {
			println!("ERR141: {}", err);
			globs.stats_counters.b_stats[i_broker].tls_err.fetch_add(1, Ordering::Relaxed);
			return Err(err);
		}
	};
	return match pair {
		DevBrokerSockets::Direct(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, globs, i_broker).await,
		DevBrokerSockets::Rustls(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, globs, i_broker).await,
		DevBrokerSockets::Rustls2Tcp(device_stream, broker_stream) => socket_forwarder2(device_stream, broker_stream, dev, globs, i_broker).await,
	};
}

async fn socket_forwarder2<T1: AsyncRead+AsyncWrite, T2: AsyncRead+AsyncWrite>(device_stream: T1, broker_stream: T2, dev: Arc<ClientInfo>, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(Result<(), String>, Result<(), String>), String> {
	let globs_orig = globs;
	let dev_orig = dev;
	let (mut ri, mut wi) = tokio::io::split(device_stream);
	let (mut ro, mut wo) = tokio::io::split(broker_stream);

	let globs = globs_orig.clone();
	let dev = dev_orig.clone();
	let client_to_server_fut = async {
		let direction = "dv->br";
		let r1 = transfer_with_sniffing(&mut ri, &mut wo, dev.clone(), direction, globs.clone(), i_broker).await;
		let ident = dev.ident.lock().await;
		println!("[{} {} {} {}] Finalizando transferência {}: {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction, r1);
		if ident.client_id.is_empty() {
			globs.stats_counters.b_stats[i_broker].tls_err.fetch_add(1, Ordering::Relaxed);
		}
		match wo.shutdown().await {
			Ok(()) => { dev.ended_server_w.store(true, Ordering::Relaxed); },
			Err(err) => { println!("[{} {} {} {}] Não foi possível dar shutdown no socket de escrita para o broker: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, err); },
		};
		r1
	};

	let globs = globs_orig.clone();
	let dev = dev_orig.clone();
	let server_to_client_fut = async {
		let direction = "dv<-br";
		let r2 = transfer_with_sniffing(&mut ro, &mut wi, dev.clone(), direction, globs, i_broker).await;
		let ident = dev.ident.lock().await;
		println!("[{} {} {} {}] Finalizando transferência {}: {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, direction, r2);
		match wi.shutdown().await {
			Ok(()) => { dev.ended_client_w.store(true, Ordering::Relaxed); },
			Err(err) => { println!("[{} {} {} {}] Não foi possível dar shutdown no socket de escrita para o device: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, err); },
		};
		r2
	};

	// tokio::try_join!(...) / tokio::select!(...)
	let (r1, r2) = tokio::join!(
		client_to_server_fut,
		server_to_client_fut,
	);

	Ok((r1, r2))
}

async fn transfer_direct<T1: AsyncRead, T2: AsyncWrite>(r: &mut ReadHalf<T1>, w: &mut WriteHalf<T2>, dev: Arc<ClientInfo>, direction: &str) -> Result<(), String> {
	let mut buffer_in = bytes::BytesMut::with_capacity(10 * 1024);
	loop {
		let read = r.read_buf(&mut buffer_in).await.map_err(|err| format!("Não foi possível ler do {}: {}", direction, err))?;
		if read != 0 {
			w.write_all(&buffer_in[(buffer_in.len()-read)..buffer_in.len()]).await.map_err(|err| format!("Não foi possível encaminhar o que veio do {}: {}", direction, err))?;
		}
		if read == 0 {
			buffer_in.clear();
			println!("[{} {} {} {}] Erro ao tentar ler do {} - Conexão encerrada", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, "", direction);
			return Ok(());
		}
		buffer_in.clear();
	}
}

async fn transfer_with_sniffing<T1: AsyncRead+Unpin, T2: AsyncWrite+Unpin>(r: &mut T1, w: &mut T2, dev: Arc<ClientInfo>, direction: &str, globs: Arc<GlobalVars>, i_broker: usize) -> Result<(), String> {
	let mut mqtt_reader = MqttPacketReader::new();
	let mut client_id = String::new();
	let mut dev_id = String::new();
	let max_packet_size = 5_000;
	let mut leu_pacote_connect = false;

	let mut ts = std::time::Instant::now();
	let mut count_pacotes = 0;

	loop {
		let mqtt_timeout = if !leu_pacote_connect {
			// Se depois de estabelecer a conexão demorar mais de 20 segundos para transmitir o primeiro pacote MQTT, a aconexão será finalizada
			20
		} else {
			// Se a conexão ficar mais de 30 minutos sem trafegar pacotes MQTT ela será finalizada
			30 * 60
		};
		let packet = match mqtt_reader.read_packet(r, max_packet_size, mqtt_timeout).await {
			Ok(packet) => {
				if client_id.is_empty() {
					let ident = dev.ident.lock().await;
					client_id = ident.client_id.clone();
					dev_id = ident.dev_id.clone();
				}

				match &packet {
					mqttbytes::v4::Packet::Connect(p) => {
						leu_pacote_connect = true;
						let mut ident = dev.ident.lock().await;
						ident.client_id = p.client_id.to_owned();
						if let Some(lw) = &p.last_will {
							if lw.message.starts_with(b"DC ") {
								if let Ok(dev_id) = std::str::from_utf8(&lw.message[3..]) {
									ident.dev_id = dev_id.to_owned();
								}
							}
						}
						client_id = ident.client_id.clone();
						dev_id = ident.dev_id.clone();
						println!("[{} {} {} {}/{}] {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, ident.dev_id, packet);
						ident.username = p.login.as_ref().and_then(|v| Some(&v.username[..])).unwrap_or("").to_owned();

						let dev_id = if ident.dev_id.len() > 0 {
							ident.dev_id.clone()
						} else {
							ident.client_id.clone()
						};
						if !dev_id.is_empty() {
							let mut devs_conn = globs.stats_counters.devs_conn.lock().await;
							match devs_conn.get_mut(&dev_id) {
								Some(counter) => {
									*counter += 1;
								},
								None => {
									devs_conn.insert(dev_id, 1);
								}
							};
						}
					},
					mqttbytes::v4::Packet::ConnAck(packet) => {
						leu_pacote_connect = true;
						// O ideal era contabilizar a conexão MQTT só aqui no ConnAck com sucesso, mas como o dev.client_id é inicializado no Connect,
						// se tiver desconexão depois de salvar o dev.client_id ela vai entrar nas estatísticas de desconexão. Então a estatística de
						// conexão tem que ser feita junto com a inicialização do dev.client_id.
						if globs.enable_debug.load(Ordering::Relaxed) == true {
							let ident = dev.ident.lock().await;
							println!("[{} {} {} {}/{}] {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, ident.client_id, ident.dev_id, packet);
						}
						let ident = dev.ident.lock().await;
						client_id = ident.client_id.clone();
						dev_id = ident.dev_id.clone();
					},
					mqttbytes::v4::Packet::Subscribe(p) => {
						for filter in &p.filters {
							if globs.enable_debug.load(Ordering::Relaxed) == true {
								println!("[{} {} {} {}] Subscribe '{}'", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, client_id, filter.path);
							}
							if filter.path == "#" {
								return Err(format!("L43 - Inscrição não permitida: {}", filter.path));
							}
							if filter.path.starts_with("data") || filter.path.starts_with("control") {
								if client_id.starts_with("DAM") && filter.path.starts_with("data/dut/DUT") {
									// DAMs are allowed to subscribe to DUT telemetry
								} else {
									return Err(format!("L43 - Inscrição não permitida: {}", filter.path));
								}
							}
							if filter.path.rfind('#').is_some() {
								return Err(format!("L43 - Inscrição não permitida: {}", filter.path));
							}
							if filter.path.rfind('+').is_some() && (!filter.path.starts_with("remota/")) {
								return Err(format!("L43 - Inscrição não permitida: {}", filter.path));
							}
						}
					},
					// dashserver, dacr2, devsv3, devkhomp, devkron
					mqttbytes::v4::Packet::Publish(p) => {
						if direction == "dv->br" {
							if p.topic.starts_with("commands") {
								// println!("[{} {} {} {}] L43 - Publicação não permitida: {} {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, dev.client_id, p.topic, String::from_utf8_lossy(p.payload.as_ref()));
								return Err(format!("L43 - Publicação não permitida: {} {}", p.topic, String::from_utf8_lossy(p.payload.as_ref())))
							} else if p.topic.starts_with("data") || p.topic.starts_with("control") {
								let allowed = client_id.starts_with("ESP32_") || p.topic.ends_with(&client_id);
								if !allowed {
									// println!("[{} {} {} {}] L43 - Publicação não permitida: {} {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, dev.client_id, p.topic, String::from_utf8_lossy(p.payload.as_ref()));
									return Err(format!("L43 - Publicação não permitida: {} {}", p.topic, String::from_utf8_lossy(p.payload.as_ref())))
								}
							}
						}
						if p.payload.len() > 5000 {
							match std::str::from_utf8(&p.payload) {
								Ok(payload) => { println!("[{} {} {} {}] Payload muito grande ({}) no tópico {}: {}...", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, client_id, p.payload.len(), p.topic, &payload[..500]); },
								Err(_err) => { println!("[{} {} {} {}] Payload muito grande ({}) no tópico {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, client_id, p.payload.len(), p.topic); },
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
							println!("[{} {} {} {}/{}] {:?}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev.addr, direction, client_id, dev_id, _packet);
						}
					},
				}

				count_pacotes += 1;
				if ts.elapsed().as_secs() >= 60 {
					if count_pacotes >= 60 {
						println!("DBG357;{};{};{};{};{}", count_pacotes, ts.elapsed().as_secs(), client_id, dev_id, dev.addr);
					}
					ts = std::time::Instant::now();
					count_pacotes = 0;
				}

				match &packet {
					mqttbytes::v4::Packet::SubAck(_p) => {
						globs.stats_counters.b_stats[i_broker].subscr.fetch_add(1, Ordering::Relaxed);
					},
					mqttbytes::v4::Packet::Publish(p) => {
						if direction == "dv<-br" {
							globs.stats_counters.b_stats[i_broker].pub_brtodv.fetch_add(1, Ordering::Relaxed);
						} else {
							globs.stats_counters.b_stats[i_broker].pub_others.fetch_add(1, Ordering::Relaxed);
						}
						if p.topic.starts_with("data/") {
							globs.stats_counters.b_stats[i_broker].topic_data.fetch_add(1, Ordering::Relaxed);
						} else if p.topic.starts_with("control/") {
							globs.stats_counters.b_stats[i_broker].topic_ctrl.fetch_add(1, Ordering::Relaxed);
						} else if p.topic.starts_with("commands/") {
							globs.stats_counters.b_stats[i_broker].topic_cmd.fetch_add(1, Ordering::Relaxed);
						}
					},
					_ => {},
				};
				packet
			},
			Err(ReadPacketResult::SocketError(err)) => {
				return Err(format!("ReadPacketResult::SocketError: {}", err));
			},
			Err(ReadPacketResult::MqttError(err)) => {
				return Err(format!("ReadPacketResult::MqttError: {}", err));
			},
			Err(ReadPacketResult::Timeout) => {
				return Err(format!("ReadPacketResult::Timeout: leu_pacote_connect={}", leu_pacote_connect));
			},
		};

		mqtt_reader.write_packet(w, packet).await?;
	}
}
