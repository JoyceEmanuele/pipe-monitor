use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use crate::mqttbytes; // ::v4::Packet;

#[derive(Debug)]
pub enum ReadPacketResult {
	SocketError(String),
	MqttError(String),
	Timeout,
}

pub struct MqttPacketReader {
	pub buffer_in: bytes::BytesMut,
	pub buffer_out: bytes::BytesMut,
}

impl MqttPacketReader {
	pub fn new() -> Self {
		return Self {
			buffer_in: bytes::BytesMut::with_capacity(10 * 1024),
			buffer_out: bytes::BytesMut::with_capacity(1 * 1024),
		};
	}
	pub async fn read_packet<T1: AsyncRead+Unpin>(&mut self, r: &mut T1, max_size: usize, timeout_secs: u64) -> Result<mqttbytes::v4::Packet, ReadPacketResult> {
		loop {
			while self.buffer_in.len() > 0 {
				match mqttbytes::v4::read(&mut self.buffer_in, max_size) {
					Err(mqttbytes::Error::InsufficientBytes(_required)) => { break; },
					Ok(packet) => { return Ok(packet); },
					Err(err) => {
						self.buffer_in.clear();
						return Err(ReadPacketResult::MqttError(format!("Não foi possível dar parse no pacote MQTT: {}", err)));
					},
				};
			}
			// if self.buffer_in.len() > 9 * 1024 {
			// 	self.buffer_in.clear();
			// 	return Err(ReadPacketResult::MqttError(format!("O pacote MQTT estourou o limite máximo de tamanho")));
			// }
			let read_socket_fut = async {
				r.read_buf(&mut self.buffer_in).await.map_err(|err| ReadPacketResult::SocketError(format!("Não foi possível ler o socket: {}", err)))
			};
			let read_result = match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), read_socket_fut).await {
				Ok(x) => x,
				Err(_err) => {
					Err(ReadPacketResult::Timeout)
				},
			};
			let read = read_result?;
			// if read != 0 {
			// 	w.write_all(&self.buffer_in[(self.buffer_in.len()-read)..self.buffer_in.len()]).await.map_err(|err| ReadPacketResult::SocketError(format!("Não foi possível escrever no socket: {}", err)))?;
			// }
			if read == 0 {
				self.buffer_in.clear();
				return Err(ReadPacketResult::SocketError(format!("O socket de leitura foi fechado (read = 0)")));
			}
		}
	}
	pub async fn write_packet<T2: AsyncWrite+Unpin>(&mut self, w: &mut T2, packet: mqttbytes::v4::Packet) -> Result<(), String> {
		serialize_packet(&packet, &mut self.buffer_out)?;
		w.write_all(&self.buffer_out[..]).await.map_err(|err| format!("Não foi possível escrever no socket: {}", err))?;
		self.buffer_out.clear();
		return Ok(());
	}
}

fn serialize_packet(packet: &mqttbytes::v4::Packet, buffer_out: &mut bytes::BytesMut) -> Result<(), String> {
	match packet {
		mqttbytes::v4::Packet::Publish(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::Subscribe(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::Connect(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::ConnAck(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PubAck(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PubRec(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PubRel(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PubComp(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::SubAck(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::Unsubscribe(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::UnsubAck(p) => { p.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PingReq => { mqttbytes::v4::PingReq.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::PingResp => { mqttbytes::v4::PingResp.write(buffer_out).map_err(|err| err.to_string())?; },
		mqttbytes::v4::Packet::Disconnect => { mqttbytes::v4::Disconnect.write(buffer_out).map_err(|err| err.to_string())?; },
	}
	return Ok(());
}
