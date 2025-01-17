use std::time::Duration;
use tokio::time::timeout;
use tokio::io::AsyncReadExt;
use super::buffer::SocketReader;
use crate::lib_log;

pub async fn read_socket_http_header(socket: &mut SocketReader, timeout_secs: u64) -> Result<(usize, usize),String> {
	// println!("Aguardando requisição - buf[{}]", data_len);
	let (i_eoh, eoh_len) = loop {
		if let Some(v) = find_eoh(&socket.buffer, std::cmp::min(socket.data_len, 1000)) {
			break v;
		}
		if socket.data_len >= 1000 {
			lib_log::append_log_tag_msg("ERROR", "ERROR23: Header too long");
			return Err("Header too long".to_owned());
		}
		read_tcp_socket_bytes(socket, timeout_secs).await?;
	};
	// println!("Chegou pacote de dados - buf[{}]", p_end);

	let h_len = i_eoh + eoh_len;

	return Ok((h_len, i_eoh));
}

async fn read_tcp_socket_bytes(socket: &mut SocketReader, timeout_secs: u64) -> Result<(),String> {
	let buffer_size = socket.buffer.len();
	if socket.data_len == buffer_size {
		lib_log::append_log_tag_msg("ERROR", "ERROR23: Insufficient buffer space");
		return Err("Insufficient buffer space".to_owned());
	}

	let read_count = match timeout(Duration::from_secs(timeout_secs), socket.stream.read(&mut socket.buffer[socket.data_len..buffer_size])).await {
		Err(err) => {
			if (socket.reqs_processed > 0) && (socket.data_len == 0) {
				return Err("Closing idle connection".to_owned());
			}
			lib_log::append_log_tag_msg("INFO", &format!("Closing idle connection after {} requests. {}", socket.reqs_processed, err));
			return Err("ERROR26 - closing idle connection".to_owned());
		},
		Ok(v) => match v {
			Err(err) => {
				lib_log::append_log_tag_msg("ERROR", &format!("ERROR28 {}", err));
				return Err("ERROR28".to_owned());
			},
			Ok(v) => v,
		},
	};
	// println!("readCount {}", read_count);
	if read_count == 0 {
		if (socket.reqs_processed > 0) && (socket.data_len == 0) {
      return Err("TCP connection closed".to_owned());
    }
		lib_log::append_log_tag_msg("ERROR", "ERROR344 task_read_client read_count == 0");
		return Err("ERROR344: read_count == 0".to_owned());
	}
	socket.data_len += read_count;

	return Ok(());
}

fn find_eoh(buffer: &[u8], data_len: usize) -> Option<(usize, usize)> {
	// let rx_hend = Regex::new(r"\r?\n\r?\n").unwrap();
	if data_len < 2 { return None; }
	for i in 1..data_len {
		if buffer[i] != 0x0A { continue; }
		if (i >= 3) && (buffer[i - 1] == 0x0D) && (buffer[i - 2] == 0x0A) && (buffer[i - 3] == 0x0D) {
			return Some((i - 3, 4));
		}
		if buffer[i - 1] == 0x0A {
			return Some((i - 1, 2));
		}
	}
	return None;
}

pub async fn read_content(socket: &mut SocketReader, content_length: usize) -> Result<Vec<u8>,String> {
	let mut content = vec![0u8; content_length];
	read_content_to(socket, &mut content).await?;
	return Ok(content);
}

async fn read_content_to(socket: &mut SocketReader, to_buffer: &mut [u8]) -> Result<usize,String> {
	let content_length: usize = to_buffer.len();
	let buffer = &mut socket.buffer;
	let data_len = &mut socket.data_len;
	let stream = &mut socket.stream;

	let i_content_start = socket.already_processed; // already_processed = header length
	let full_packet_len = i_content_start + content_length; // full_packet_len = HTTP header + content
	let mut content_length_read = 0;

	{
		let extra_bytes = *data_len - i_content_start; // extra_bytes = how many bytes are already in the buffer
		let content_in_buff = std::cmp::min(content_length, extra_bytes);
		if content_in_buff > 0 {
			to_buffer[0..content_in_buff].copy_from_slice(&buffer[i_content_start..i_content_start+content_in_buff]);
			content_length_read += content_in_buff;
		}
	}

	if *data_len > full_packet_len {
		let extra_bytes = *data_len - full_packet_len;
		for i in 0..extra_bytes {
			buffer[i] = buffer[full_packet_len + i];
		}
		*data_len = extra_bytes;
	} else {
		*data_len = 0;
	}
	socket.already_processed = 0;

	loop {
		if content_length_read >= content_length {
			return Ok(content_length_read);
		}
		let read_count = match timeout(
			Duration::from_secs(150),
			stream.read(&mut to_buffer[content_length_read..content_length])
		).await {
			Err(_err) => { return Err("ERROR21 - closing idle connection".to_owned()) },
			Ok(v) => match v {
				Ok(v) => v,
				Err(err) => { return Err(format!("ERROR89: {}", err)) },
			},
		};
		// println!("readCount {}", read_count);
		if read_count == 0 {
			return Err("ERROR92".to_owned());
		}
		content_length_read += read_count;
	}
}
