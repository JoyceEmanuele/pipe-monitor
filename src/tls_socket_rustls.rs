use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use crate::client_cert_checker::DielClientCertChecker;
use crate::configs::ConfigFile;
use crate::GlobalVars;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;

fn load_public_cert_file(configfile: &ConfigFile) -> Result<Vec<CertificateDer<'static>>, String> {
	// Get certificates
	let cert_path = &configfile.CERT_PATH;
	let cert_file = File::open(&cert_path);
	let cert_file = cert_file.map_err(|err| format!("Error::ServerCertNotFound({})\n{:?}", cert_path, err))?;
	let mut reader = BufReader::new(cert_file);
	let certs = rustls_pemfile::certs(&mut reader);
	let certs: Vec<CertificateDer<'static>> = certs.filter_map(|c| c.ok()).collect();
	// let certs = certs.map_err(|err| format!("Error::InvalidServerCert - certs ({})\n{:?}", cert_path, err))?;
	// let certs = certs
	// 	.iter()
	// 	.map(|cert| Certificate(cert.to_owned()))
	// 	.collect();
	Ok(certs)
}

fn load_private_key_file(configfile: &ConfigFile) -> Result<PrivateKeyDer<'static>, String> {
	// Get private key
	let key_path = &configfile.KEY_PATH;
	{
		// use std::iter;
		// use rustls_pemfile::{Item, read_one};
		// let key_file = File::open(&key_path);
		// let key_file = key_file.map_err(|err| format!("ServerKeyNotFound({})\n{:?}", key_path, err))?;
		// let mut reader = BufReader::new(key_file);
		// // Assume `reader` is any std::io::BufRead implementor
		// for item in iter::from_fn(|| read_one(&mut reader).transpose()) {
		// 		match item.unwrap() {
		// 				Item::X509Certificate(cert) => println!("certificate {:?}", cert),
		// 				Item::RSAKey(key) => println!("rsa pkcs1 key {:?}", key),
		// 				Item::PKCS8Key(key) => println!("pkcs8 key {:?}", key),
		// 				Item::ECKey(key) => println!("sec1 ec key {:?}", key),
		// 				_ => println!("unhandled item"),
		// 		}
		// }
	}
	let key_file = File::open(&key_path);
	let key_file = key_file.map_err(|err| format!("ServerKeyNotFound({})\n{:?}", key_path, err))?;
	// let keys = rustls_pemfile::rsa_private_keys(&mut BufReader::new(key_file));
	// let keys = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(key_file));
	// let keys = std::iter::from_fn(|| rustls_pemfile::read_one(&mut BufReader::new(key_file)).transpose()).next();
	let keys = rustls_pemfile::read_one(&mut BufReader::new(key_file)).transpose();
	let keys = keys.ok_or("Error::InvalidServerKey - read_one".to_owned())?;
	let keys = keys.map_err(|err| format!("Error::InvalidServerKey - {}", err))?;
	let key = match keys {
		rustls_pemfile::Item::Pkcs8Key(key) => key,
		_ => { return Err("Error::InvalidServerKey - invalid key type".to_owned()); },
	};
	// let keys = keys.map_err(|err| format!("Error::InvalidServerKey - rsa_private_keys ({})\n{:?}", key_path, err))?;

	// // Get the first key
	// let key = match keys.first() {
	// 	Some(k) => k.clone(),
	// 	None => return Err(format!("Error::InvalidServerKey - no first ({})", key_path)),
	// };
	Ok(PrivateKeyDer::Pkcs8(key))
}

fn load_ca_file_for_client_auth(configfile: &ConfigFile) -> Result<(RootCertStore, Vec<Vec<u8>>), String> {
	// client authentication with a CA. CA isn't required otherwise
	let ca_path = &configfile.CA_PATH;
	let ca_file = File::open(&ca_path);
	let ca_file = ca_file.map_err(|err| format!("CaFileNotFound {}\n{}", ca_path, err))?;
	let ca_file = &mut BufReader::new(ca_file);
	let ca_certs = rustls_pemfile::certs(ca_file);

	let mut store = RootCertStore::empty();
	let mut as_vec: Vec<Vec<u8>> = Vec::new();

	for c in ca_certs {
		let c = c.map_err(|e| format!("ERR223 {}", e))?;
		as_vec.push(c.to_vec());
		store
			.add(c)
			.map_err(|e| format!("ERR222 {}", e))?;
	}

	Ok((store, as_vec))
}
// fn create_root_store() {
//   let mut root_store = rustls::RootCertStore::empty();
//   root_store.extend(
//       webpki_roots::TLS_SERVER_ROOTS
//           .iter()
//           .cloned()
//   );
// }

pub fn create_rustls_config(configfile: &ConfigFile) -> Result<(tokio_rustls::TlsAcceptor, Vec<Vec<u8>>), String> {
	tokio_rustls::rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
	let (roots_for_client_cert, roots_as_vec) = load_ca_file_for_client_auth(configfile)?;

	let server_config = {
		let certs = load_public_cert_file(configfile)?;
		let key = load_private_key_file(configfile)?;
		ServerConfig::builder()
			// .with_safe_defaults()
			.with_client_cert_verifier(DielClientCertChecker::new(roots_for_client_cert))
			// .with_client_cert_verifier(AllowAnyAnonymousOrAuthenticatedClient::new(store))
			// .with_client_cert_verifier(NoClientAuth::new())
			.with_single_cert(certs, key)
			.expect("bad certificate/key")
	};
	let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

	return Ok((acceptor, roots_as_vec));
}

pub async fn stablish_incoming_socket(device_stream: tokio::net::TcpStream, dev_addr: &str, globs: &Arc<GlobalVars>) -> Result<tokio_rustls::TlsStream<tokio::net::TcpStream>, String> {
	let device_stream = globs.rustls_acceptor.accept(device_stream)
	// let device_stream = acceptor.accept_with(device_stream, |session| {
	// 	println!("session: {:?}", session);
	// 	session.complete_io(&mut device_stream);
	// 	// device_stream.cert;
	// })
		.await
		.map_err(|err| format!("Failed to accept TLS connection using Rustls. Error = {:?}", err))?;
	let enable_debug = globs.enable_debug.load(Ordering::Relaxed);
	if enable_debug {
		let (_stream, connection) = device_stream.get_ref();
		if let Some(certs) = connection.peer_certificates() {
			if certs.len() == 1 {
				for cert in certs {
					let validation = crate::client_cert_checker::check_expired_cert_2(&globs.rustls_ca_roots, cert, &[]);
					match validation {
						Ok(v) => { println!("[{} {}] Conexão trouxe certificado: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev_addr, v); },
						Err(err) => { println!("[{} {}] Conexão trouxe certificado inválido: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev_addr, err); },
					}
				}
			} else {
				println!("[{} {}]Conexão trouxe uma quantidade inválida de certificados: {}", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev_addr, certs.len());
				for cert in certs {
					let validation = crate::client_cert_checker::check_expired_cert_2(&globs.rustls_ca_roots, cert, &[]);
					match validation {
						Ok(v) => { println!(" - certificado: {}", v); },
						Err(err) => { println!(" - certificado inválido: {}", err); },
					}
				}
			}
		} else {
			println!("[{} {}] Conexão não trouxe certificado", &(chrono::offset::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[..19]), dev_addr);
		}
	}

	return Ok(device_stream.into());
}

fn create_client_config(configfile: &ConfigFile) -> Result<tokio_rustls::rustls::ClientConfig, String> {
	// Utilizado para abrir uma conexão TLS com o broker
	let root_store = load_ca_file_for_broker(configfile)?;
	let root_store = Arc::new(root_store);
	let client_config = ClientConfig::builder()
		// .with_safe_defaults()
		.with_root_certificates(root_store)
		.with_no_client_auth();
	// let client_config = Arc::new(client_config);
	Ok(client_config)
}

fn load_ca_file_for_broker(configfile: &ConfigFile) -> Result<RootCertStore, String> {
	let mut root_store = RootCertStore::empty();
	// root_store.add_server_trust_anchors(
	//     webpki_roots::TLS_SERVER_ROOTS
	//         .0
	//         .iter()
	//         .map(|ta| {
	//             OwnedTrustAnchor::from_subject_spki_name_constraints(
	//                 ta.subject,
	//                 ta.spki,
	//                 ta.name_constraints,
	//             )
	//         }),
	// );
	let ca_path = &configfile.CA_PATH;
	let ca_file = File::open(&ca_path);
	let ca_file = ca_file.map_err(|err| format!("CaFileNotFound {}\n{}", ca_path, err))?;
	let ca_file = &mut BufReader::new(ca_file);
	let ca_certs = rustls_pemfile::certs(ca_file);
	// let ca_cert = ca_certs
	//     .first()
	//     .map(|c| Certificate(c.to_owned()))
	//     .ok_or_else(|| Error::InvalidCACert(ca_path.to_string())).map_err(|e| format!("{}", e))?;
	// // let mut store = RootCertStore::empty();
	// root_store
	//     .add(&ca_cert)
	//     .map_err(|_| Error::InvalidCACert(ca_path.to_string())).map_err(|e| format!("{}", e))?;

	for c in ca_certs {
		// let ta = webpki::TrustAnchor::try_from_cert_der(&c).map_err(|e| format!("{}", e))?;
		// let ota = OwnedTrustAnchor::from_subject_spki_name_constraints(
		//     ta.subject,
		//     ta.spki,
		//     ta.name_constraints,
		// );
		// println!("{:?}", ota);
		root_store
			.add(c.map_err(|e| format!("ERR223 {}", e))?)
			.map_err(|e| format!("ERR222 {}", e))?;
	}
	// let ca_certs2 = ca_certs
	//     .iter()
	//     .map(|der| {
	//         let ta = webpki::TrustAnchor::try_from_cert_der(&der.0).expect("ERROR in TrustAnchor::try_from_cert_der");
	//         let ota = OwnedTrustAnchor::from_subject_spki_name_constraints(
	//             ta.subject,
	//             ta.spki,
	//             ta.name_constraints,
	//         );
	//         ota
	//     })
	//     .ok_or_else(|| Error::InvalidCACert(ca_path.to_string())).map_err(|e| format!("{}", e))?;
	// // let mut store = RootCertStore::empty();
	Ok(root_store)
}
