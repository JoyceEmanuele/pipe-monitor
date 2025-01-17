use std::collections::HashMap;
use tokio::net::TcpStream;
use regex::Regex;
use std::str::FromStr;
use tokio::io::AsyncWriteExt;
use super::buffer::SocketReader;
use super::protocol::{read_socket_http_header, read_content};
use super::types::{HttpHeaderEntry, HttpResponse};
use serde::Serialize;
use crate::lib_log;

pub async fn send_response(socket: &mut TcpStream, response: &HttpResponse) -> Result<(), String> { // mut socket: tokio::io::WriteHalf<tokio::net::TcpStream>
	// stream_w: Arc<tokio::sync::Mutex<tokio::io::WriteHalf<tokio::net::TcpStream>>>
	// let mut socket = stream_w.lock().await;
	// if let Err(err) = socket.write_all("Olá".to_owned().as_bytes()).await {
	// 	 println!("{}", err);
	// }

	socket
		.write_all(build_res_header_str(response).as_bytes())
		.await
		.map_err(|err| format!("Error writing data to socket: {}", err))?;
	if response.content.len() > 0 {
		socket
			.write_all(&response.content)
			.await
			.map_err(|err| format!("Error writing data to socket: {}", err))?;
		if response.content.len() <= 800 {
			lib_log::append_log_tag_msg("INFO", &format!(
				"DBG response {} {}",
				response.status_code,
				String::from_utf8_lossy(&response.content)
			));
		} else {
			lib_log::append_log_tag_msg("INFO", &format!("DBG response {}", response.status_code));
		}
	}
	else {
		lib_log::append_log_tag_msg("INFO", "DBG empty response, breaking TCP connection");
	}
	socket.flush().await.map_err(|err| format!("Error flushing data to socket: {}", err))?;

  return Ok(());
}

fn build_res_header_str(response: &HttpResponse) -> String {
	let mut header = String::with_capacity(200);
	header += &format!("HTTP/1.1 {} {}\r\n", &response.status_code.to_string(), &response.status_desc);
	for (attribute, value) in response.headers.iter() {
		header += &format!("{}: {}\r\n", attribute, value);
	}
	header += "\r\n";
	return header;
}

pub fn build_http_response(status_code: u16, content_bytes: Vec<u8>, content_type: &str) -> HttpResponse {
	// let content_bytes = content_str.as_bytes().to_vec();
	// let content_bytes = content_json.dump().as_bytes().to_vec();
	// let content_bytes = content_json.to_string().as_bytes().to_vec();
  let mut headers = HashMap::new();
  headers.insert("Content-Type".to_owned(), content_type.to_owned());
  headers.insert("Content-Length".to_owned(), content_bytes.len().to_string());
	return HttpResponse {
		status_code,
		status_desc: status_code_desc(status_code),
		headers,
		content: content_bytes,
	};
}

fn status_code_desc(status_code: u16) -> &'static str {
	return match status_code {
		200 => "OK",
		400 => "Bad Request",
		403 => "Forbidden",
		404 => "Not Found",
		500 => "Internal Server Error",
		501 => "Not Implemented",
		505 => "HTTP Version Not Supported",
		_ => "Unknown",
	};
}

fn extension_mimetype(ext: &str) -> &'static str {
	return match ext {
		"aac" => "audio/aac",
		"abw" => "application/x-abiword",
		"arc" => "application/x-freearc",
		"avi" => "video/x-msvideo",
		"azw" => "application/vnd.amazon.ebook",
		"bin" => "application/octet-stream",
		"bmp" => "image/bmp",
		"bz" => "application/x-bzip",
		"bz2" => "application/x-bzip2",
		"cda" => "application/x-cdf",
		"csh" => "application/x-csh",
		"css" => "text/css",
		"csv" => "text/csv",
		"doc" => "application/msword",
		"docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"eot" => "application/vnd.ms-fontobject",
		"epub" => "application/epub+zip",
		"gz" => "application/gzip",
		"gif" => "image/gif",
		"htm" => "text/html",
		"html" => "text/html",
		"ico" => "image/vnd.microsoft.icon",
		"ics" => "text/calendar",
		"jar" => "application/java-archive",
		"jpeg" => "image/jpeg",
		"jpg" => "image/jpeg",
		"js" => "text/javascript",
		"json" => "application/json",
		"jsonld" => "application/ld+json",
		"mid" => "audio/midi",
		"midi" => "audio/midi",
		"mjs" => "text/javascript",
		"mp3" => "audio/mpeg",
		"mp4" => "video/mp4",
		"mpeg" => "video/mpeg",
		"mpkg" => "application/vnd.apple.installer+xml",
		"odp" => "application/vnd.oasis.opendocument.presentation",
		"ods" => "application/vnd.oasis.opendocument.spreadsheet",
		"odt" => "application/vnd.oasis.opendocument.text",
		"oga" => "audio/ogg",
		"ogv" => "video/ogg",
		"ogx" => "application/ogg",
		"opus" => "audio/opus",
		"otf" => "font/otf",
		"png" => "image/png",
		"pdf" => "application/pdf",
		"php" => "application/x-httpd-php",
		"ppt" => "application/vnd.ms-powerpoint",
		"pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"rar" => "application/vnd.rar",
		"rtf" => "application/rtf",
		"sh" => "application/x-sh",
		"svg" => "image/svg+xml",
		"swf" => "application/x-shockwave-flash",
		"tar" => "application/x-tar",
		"tif" => "image/tiff",
		"tiff" => "image/tiff",
		"ts" => "video/mp2t",
		"ttf" => "font/ttf",
		"txt" => "text/plain",
		"vsd" => "application/vnd.visio",
		"wav" => "audio/wav",
		"weba" => "audio/webm",
		"webm" => "video/webm",
		"webp" => "image/webp",
		"woff" => "font/woff",
		"woff2" => "font/woff2",
		"xhtml" => "application/xhtml+xml",
		"xls" => "application/vnd.ms-excel",
		"xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"xml" => "application/xml",
		"xul" => "application/vnd.mozilla.xul+xml",
		"zip" => "application/zip",
		"3gp" => "video/3gpp",
		"3g2" => "video/3gpp2",
		"7z" => "application/x-7z-compressed",
		_ => "unknown",
	};
}

pub fn respond_http_plain_text(status_code: u16, content_str: &str) -> HttpResponse {
	return build_http_response(status_code, content_str.as_bytes().to_vec(), "text/plain; charset=UTF-8");
}
pub fn respond_http_html(status_code: u16, content_str: &str) -> HttpResponse {
	return build_http_response(status_code, content_str.as_bytes().to_vec(), "text/html; charset=UTF-8");
}
pub fn respond_http_json(status_code: u16, content_str: &str) -> HttpResponse {
	return build_http_response(status_code, content_str.as_bytes().to_vec(), "application/json; charset=UTF-8");
}
pub fn respond_http_file(status_code: u16, content_bytes: Vec<u8>, extension: &str) -> HttpResponse {
	return build_http_response(status_code, content_bytes, extension_mimetype(extension));
}
// pub fn respond_http_json(status_code: u16, content: json::JsonValue) -> HttpResponse {
// 	let content_bytes = content.dump().as_bytes().to_vec();
// 	return build_http_response(status_code, content_bytes, "application/json; charset=UTF-8");
// }
// pub fn respond_http_json(status_code: u16, content_bytes: Vec<u8>) -> HttpResponse {
// 	return build_http_response(status_code, content_bytes, "application/json; charset=UTF-8");
// }
// pub fn respond_http_plain_text(status_code: u16, content_bytes: Vec<u8>) -> HttpResponse {
// 	return build_http_response(status_code, content_bytes, "text/plain; charset=UTF-8");
// }

pub fn respond_http_json_serializable<T: Serialize>(status_code: u16, content: T) -> HttpResponse {
	let content_bytes =
			serde_json::to_vec(&content).map_err(|e| format!("{{\"error\" : \"{}\"}}", e));
	let status_code = if content_bytes.is_ok() {
			status_code
	} else {
			500
	};
	let content_bytes = match content_bytes {
			Ok(x) => x,
			Err(x) => x.into_bytes(),
	};
	return build_http_response(status_code, content_bytes, "application/json; charset=UTF-8");
}

#[derive(Debug)]
pub struct ResponseHeader {
	pub status_code: u16,
	pub status_desc: String,
	pub headers: HashMap<String,String>,
	pub content_length: usize,
}

fn parse_http_res_header(buffer: &[u8]) -> Result<ResponseHeader,String> {
	let header = match std::str::from_utf8(&buffer[..]) {
		Ok(v) => v,
		Err(err) => { return Err(format!("ERROR49: {}", err)); },
	};
	let mut lines = header.lines(); // : std::str::Lines<'a>

	let first_line = match lines.next() {
		Some(v) => v,
		None => { return Err("ERROR39: no first line".to_owned()); }
	};

	let rx_get = Regex::new(r"^HTTP/([\d\.]+) (\d+) ([^\n^\r]+)\r?\n$").unwrap();
	let matched = match rx_get.captures(first_line) {
		Some(v) => v,
		None => {
			return Err(format!("Invalid response first line: {}", first_line));
		}
	};
	let _http_v = matched[1].to_owned();
	let status_code = matched[2].to_owned();
	let status_desc = matched[3].to_owned();

  let status_code = u16::from_str(&matched[2])
    .map_err(|err| format!("Invalid status code: {}", err))?;

	let mut headers: HashMap<String,String> = HashMap::new();
	let mut content_length = 0;

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
		headers.insert(header.attribute, header.value);
	}

	return Ok(ResponseHeader{
		status_code,
		status_desc,
		headers,
		content_length,
	});
}

pub async fn read_socket_http_response(socket: &mut SocketReader, max_res_size: Option<usize>) -> Result<HttpResponse,String> {
	let (h_len, i_eoh) = read_socket_http_header(socket, 150).await?;
	let res_header = parse_http_res_header(&socket.buffer[0..i_eoh])?;
	socket.reqs_processed += 1;
	socket.already_processed = h_len;

	// let mut req_data_part_len = std::cmp::min(full_packet_len, buffer_size);
	if let Some(max_res_size) = max_res_size {
		let full_packet_len = h_len + res_header.content_length;
		if full_packet_len > max_res_size {
			return Err("ERROR81: response too big".to_owned());
		}
	}

	let content = read_content(socket, res_header.content_length).await?;

	let res = HttpResponse {
		status_code: res_header.status_code,
		status_desc: status_code_desc(res_header.status_code),
		headers: res_header.headers,
		content,
	};

	return Ok(res);
}
