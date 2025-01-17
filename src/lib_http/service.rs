use std::time::Duration;
use super::buffer::SocketReader;
use super::response::{send_response, respond_http_plain_text};
use super::request::read_socket_http_request;

pub fn run_service(bind_addr: &str) {
	// let bind_addr = "127.0.0.1:46878"; // configfile::LISTEN_SOCKET_HIST
	loop {
		let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Error creating tokio runtime");
		let result = rt.block_on(async {
			let listener = tokio::net::TcpListener::bind(bind_addr)
				.await
				.expect("Error binding to TCP port");
			println!("Awaiting HTTP clients");

			loop {
				let (socket, _) = match listener.accept().await {
					Ok(v) => v,
					Err(err) => {
						println!("Error getting incoming TCP stream: {}", err);
						continue;
					}
				};
				// println!("Cliente HTTP conectado {}", socket.peer_addr().expect("Error getting peer address"));
				handle_http_request(socket).await;
				// tokio::spawn(handle_http_request(socket));
			}
		});
		println!("http task ended: {:?}", result);
		std::thread::sleep(Duration::from_secs(60));
	}
}

async fn handle_http_request(socket: tokio::net::TcpStream) {
	// TODO: start a new thread to handle the request, but wait for it to finish before accepting a new request. This is to handle panics.
	let origin: String = socket.peer_addr().expect("Error getting peer address").to_string();
	// let (socket_read, socket_write) = tokio::io::split(socket);
	let mut socket_reader = SocketReader::new(socket, 1000);
	loop {
		let req = match read_socket_http_request(&mut socket_reader, Some(2_000_000)).await {
			Ok(v) => v,
			Err(err) => {
				// send_response(&mut socket_reader.stream, &respond_http_plain_text(500, &err)).await;
				println!("Connection ended: {}", err);
				return;
			},
		};
		println!(
			"DBG request {} {} {} {}",
			req.method,
			req.path,
			origin,
			String::from_utf8_lossy(&req.content)
		);
		let response = match crate::lib_http::api::process_req(&req).await {
			Ok(v) => v,
			Err(err) => respond_http_plain_text(500, &err),
		};
		if let Err(err) = send_response(&mut socket_reader.stream, &response).await {
			println!("ERR61 - {}", err);
		}
		return;
	}
}
