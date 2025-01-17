use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use regex::Regex;
use std::str::FromStr;
use tokio::io::AsyncWriteExt;
use super::buffer::SocketReader;
use super::protocol::{read_socket_http_header, read_content};
use super::response::read_socket_http_response;
use super::types::{HttpHeaderEntry, HttpResponse, HttpRequest};
use crate::lib_log;

pub async fn read_socket_http_request(mut socket: &mut SocketReader, max_req_size: Option<usize>) -> Result<HttpRequest,String> {
	let (h_len, i_eoh) = read_socket_http_header(&mut socket, 150).await?;
	let req_header = parse_http_req_header(&socket.buffer[0..i_eoh])?;
	socket.reqs_processed += 1;
	socket.already_processed = h_len;

	// let mut req_data_part_len = std::cmp::min(full_packet_len, buffer_size);
	if let Some(max_req_size) = max_req_size {
		let full_packet_len = h_len + req_header.content_length;
		if full_packet_len > max_req_size {
			return Err("ERROR81: requisition too big".to_owned());
		}
	}

	let content = read_content(socket, req_header.content_length).await?;

	let req = HttpRequest {
		method: req_header.method,
		path: req_header.path,
		// req_id: req_header.req_id,
		headers: req_header.headers,
		content,
	};

	return Ok(req);
}

#[derive(Debug)]
pub struct RequestHeader {
	pub method: String,
	pub path: String,
	pub headers: HashMap<String,String>, // Vec<HttpHeaderEntry>
	pub content_length: usize,
	// pub h_len: usize,
	// pub p_len: usize,
}

fn parse_http_req_header(buffer: &[u8]) -> Result<RequestHeader,String> {
	let header = match std::str::from_utf8(&buffer[..]) {
		Ok(v) => v,
		Err(err) => { return Err(format!("ERROR49: {}", err)); },
	};
	let mut lines = header.lines(); // : std::str::Lines<'a>

	let first_line = match lines.next() {
		Some(v) => v,
		None => { return Err("ERROR39: no first line".to_owned()); }
	};

	let rx_get = Regex::new(r"^(\w+) ([^\n^\s]+) HTTP/([\d\.]+)").unwrap();
	let matched = match rx_get.captures(first_line) {
		Some(v) => v,
		None => {
			lib_log::append_log_tag_msg("ERROR", &format!("ERROR44: {}", &first_line));
			return Err("ERROR44".to_owned());
		}
	};
	let method = matched[1].to_owned(); // .len();
	let path = matched[2].to_owned(); // method_end + 1 + matched[2].len();
	let _http_v = matched[3].to_owned();

	// let mut headers_i = Vec::<(usize,usize,usize,usize)>::new();
	let mut headers: HashMap<String,String> = HashMap::new(); // Vec::<HttpHeaderEntry>::new();
	// let mut headers = Vec::<(&'a str, &'a str)>::new();
	let mut content_length = 0;
	// let mut req_id = String::new();

	while let Some(line) = lines.next() {
		let i = match line.find(':') {
			Some(v) => v,
			None => {
				return Err("ERROR57".to_owned());
			}
		};
		let header = HttpHeaderEntry {
			attribute: line[0..i].trim().to_lowercase(),
			value: line[i + 1..].trim().to_owned(),
		};
		if header.attribute == "content-length" {
			content_length = match usize::from_str(&header.value) {
				Ok(v) => v,
				Err(_) => {
					return Err("ERROR66".to_owned());
				}
			};
		}
		// else if (header.attribute == "req-id") {
		// 	req_id.clear();
		// 	req_id += &header.value;
		// }
		// println!("'{}': '{}'", header.attribute, header.value);
		headers.insert(header.attribute, header.value);
		// let p1: &'a str = line[0..i].trim();
		// let p2: &'a str = line[0..i].trim();
		// headers_i.push((0, i, i + 1, line.len()));
		// if (line[0..i].trim().to_lowercase() == "content-length") {
		// 	content_length = match usize::from_str(line[i + 1..].trim()) { Ok(v) => v, Err(_) => { return Err("ERROR66".to_owned()); } };
		// }
	}

	// let i_content_start = i_eoh + eoh_len;
	// let i_content_start = req_header.h_len;
	// let content_length = req_header.p_len;
	// let full_packet_len = h_len + content_length;
	return Ok(RequestHeader{
		method,
		path,
		headers,
		content_length,
		// h_len: i_content_start,
		// p_len: content_length,
	});
}

pub async fn do_http_request(addr: &str, request: &HttpRequest) -> Result<HttpResponse, String> {
	let mut socket = tokio::net::TcpStream::connect(addr).await
		.map_err(|err| format!("Error connecting to API-Server: {}", err))?;

	// println!("Connected to API-Server");
	send_request(&mut socket, request).await
		.map_err(|err| format!("Não foi possível enviar a requisição: {}", err))?;

	let mut socket = SocketReader::new(socket, 1020);

	// O que chega na resposta é um json com o conteúdo do arquivo com
	// as tabelas criadas automaticamente. Salvar o arquivo localmente.

	let response = {
		let result = tokio::time::timeout(
			Duration::from_secs(30),
			read_socket_http_response(&mut socket, Some(10_000_000))
		)
			.await
			.map_err(|_e| "Tempo expirado aguardando resposta do API-Server".to_owned())
			.and_then(|v| v);
		result.map_err(|err| format!("ERR179 {}", err))?
	};

	return Ok(response);
}

fn build_req_header_str(request: &HttpRequest) -> String {
	let mut header = String::with_capacity(200);
	header += &format!("{} {} HTTP/1.1\r\n", &request.method, &request.path);
	for (attribute, value) in request.headers.iter() {
		header += &format!("{}: {}\r\n", attribute, value);
	}
	header += "\r\n";
	return header;
}

async fn send_request(socket: &mut TcpStream, request: &HttpRequest) -> Result<(), String> {
	socket
		.write_all(build_req_header_str(request).as_bytes())
		.await
		.map_err(|err| format!("Error writing data to socket: {}", err));
	if request.content.len() > 0 {
		socket
			.write_all(&request.content)
			.await
			.map_err(|err| format!("Error writing data to socket: {}", err));
	}

	if (request.content.len() > 0) && (request.content.len() <= 200) {
		lib_log::append_log_tag_msg("INFO", &format!(
			"DBG request {} {}",
			request.path,
			String::from_utf8_lossy(&request.content)
		));
	} else {
		lib_log::append_log_tag_msg("INFO", &format!("DBG request {}", request.path));
	}

	socket.flush().await.map_err(|err| format!("Error flushing data to socket: {}", err));

	return Ok(());
}
