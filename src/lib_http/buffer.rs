use tokio::net::TcpStream;
use tokio::io::ReadHalf;

pub struct SocketReader {
	pub stream: TcpStream, // ReadHalf<TcpStream>,
	pub buffer: Vec<u8>,
	// pub buff_capacity: usize,
	pub data_len: usize,
	pub reqs_processed: usize,
	pub already_processed: usize,
}
impl SocketReader {
	pub fn new(socket: TcpStream, buff_capacity: usize) -> Self { // socket_read: ReadHalf<TcpStream>
		SocketReader {
			stream: socket,
			buffer: vec!(0u8; buff_capacity),
			// buff_capacity,
			data_len: 0,
			reqs_processed: 0,
			already_processed: 0,
		}
	}
	pub fn get_socket(self) -> TcpStream {
		self.stream
	}
}

pub struct SocketBuffer {
	pub stream: Option<ReadHalf<TcpStream>>,
	pub buffer: Vec<u8>,
	pub data_len: usize,
	pub reqs_processed: usize,
}
impl SocketBuffer {
	pub fn new(socket_read: ReadHalf<TcpStream>, buff_capacity: usize) -> Self {
		SocketBuffer {
			stream: Some(socket_read),
			buffer: vec!(0u8; buff_capacity),
			data_len: 0,
			reqs_processed: 0,
		}
	}
	pub fn from_vec(bytes: Vec<u8>) -> Self {
		let buff_capacity = bytes.len();
		SocketBuffer {
			stream: None,
			buffer: bytes,
			data_len: 0,
			reqs_processed: 0,
		}
	}
}
