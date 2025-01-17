use std::collections::HashMap;

pub struct HttpHeaderEntry {
	pub attribute: String,
	pub value: String,
}
impl HttpHeaderEntry {
	pub fn from_str(header: &str, value: &str) -> Self {
		return HttpHeaderEntry {
			attribute: header.to_owned(),
			value: value.to_owned(),
		}
	}
}

pub struct HttpRequest {
	pub method: String,
	pub path: String,
	// pub req_id: String,
	pub headers: HashMap<String,String>, // Vec<HttpHeaderEntry>,
	pub content: Vec<u8>,
}

pub struct HttpResponse {
	pub status_code: u16,
	pub status_desc: &'static str,
	pub headers: HashMap<String,String>,
	pub content: Vec<u8>,
}

#[derive(Debug)]
pub struct HTTPEResponse {
	pub headers: HashMap<String,String>,
	pub content: Vec<u8>,
}

pub struct HttpeRequest {
	pub data: Vec<u8>,
	pub headers_i: Vec<(usize,usize,usize,usize)>,
	pub method_i: Option<(usize,usize)>,
	pub path_i: Option<(usize,usize)>,
	pub content_i: Option<(usize,usize)>,
}
impl HttpeRequest {
	// pub fn from_http(data: Vec<u8>, h_len: usize) -> Result<Self, String> {
	// 	let header_str = match std::str::from_utf8(&data[0..h_len]) {
			
	// 	}
	// 	if let Err(err) =  {
	// 		return Err(format!("ERROR45: {}", err));
	// 	};
	// 	let mut req = Self{
	// 		data,
	// 		headers_i: Vec::new(),
	// 		path_i: None,
	// 		method_i: None,
	// 	};
	// 	let mut line_start = 0;
	// 	for i in 0..h_len {
	// 		if req.data[i] == b'\n' {
	// 			if req.path_i.is_none() {
	// 				let first_line = std::str
	// 				let rx_get = Regex::new(r"^(\w+) ([^\n^\s]+) HTTP/([\d\.]+)").unwrap();
	// 				let matched = match rx_get.captures(&firstLine) { Some(v) => v, None => { println!("{}", &firstLine); return Err("ERROR44".to_owned()); } };
	// 				let method = matched[1].to_owned();
	// 				let path = matched[2].to_owned();
	// 				let http_v = matched[3].to_owned();
				
	// 			} else {
	// 				req.headers_i.push((line_start, i, 0 , 0));
	// 			}
	// 			line_start = i + 1;
	// 		}
	// 	}
	// 	return Ok(req);
	// }
	pub fn get_header(&self, name: &str) -> Option<&str> {
		for (n_start, n_end, v_start, v_end) in &self.headers_i {
			if std::str::from_utf8(&self.data[*n_start..*n_end]).unwrap() == name {
				return Some(std::str::from_utf8(&self.data[*v_start..*v_end]).unwrap());
			}
		}
		return None;
	}
	pub fn get_path(&self) -> &str {
		match &self.path_i {
			None => { return ""; },
			Some((i_start, i_end)) => {
				return std::str::from_utf8(&self.data[*i_start..*i_end]).unwrap();
			},
		}
	}
	pub fn get_content(&self) -> (&[u8], usize) {
		match &self.content_i {
			None => { return (&self.data[0..0], 0); },
			Some((i_start, i_end)) => {
				return (&self.data[*i_start..std::cmp::min(*i_end, self.data.len())], *i_end - *i_start);
			},
		}
	}
	pub fn get_method(&self) -> &str {
		match &self.method_i {
			None => { return ""; },
			Some((i_start, i_end)) => {
				return std::str::from_utf8(&self.data[*i_start..*i_end]).unwrap();
			},
		}
	}
}
// impl<'a> HttpeRequest<'a> {
// 	pub fn push_header_line(&'a mut self, line_start: usize, line_length: usize) {
// 		self.header_lines.push(std::str::from_utf8(&self.data[line_start..line_start+line_length]).unwrap());
// 	}
// 	pub fn parse_header_lines(&'a mut self, h_len: usize) {
// 		self.header_lines = Vec::new();
// 		let mut line_start = 0;
// 		for i in 0..h_len {
// 			if self.data[i] == b'\n' {
// 				self.header_lines.push(std::str::from_utf8(&self.data[line_start..i]).unwrap());
// 				line_start = i + 1;
// 			}
// 		}
// 	}
// }
