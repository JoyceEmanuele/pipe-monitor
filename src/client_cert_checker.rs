use tokio_rustls::rustls::pki_types::{CertificateDer, UnixTime};
use tokio_rustls::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
	RootCertStore,
	DistinguishedName,
	Error,
};
use std::sync::Arc;
use std::time::{SystemTime, Duration};

static SUPPORTED_SIG_ALGS: &[&webpki::SignatureAlgorithm] = &[
	&webpki::ECDSA_P256_SHA256,
	&webpki::ECDSA_P256_SHA384,
	&webpki::ECDSA_P384_SHA256,
	&webpki::ECDSA_P384_SHA384,
	&webpki::ED25519,
	&webpki::RSA_PSS_2048_8192_SHA256_LEGACY_KEY,
	&webpki::RSA_PSS_2048_8192_SHA384_LEGACY_KEY,
	&webpki::RSA_PSS_2048_8192_SHA512_LEGACY_KEY,
	&webpki::RSA_PKCS1_2048_8192_SHA256,
	&webpki::RSA_PKCS1_2048_8192_SHA384,
	&webpki::RSA_PKCS1_2048_8192_SHA512,
	&webpki::RSA_PKCS1_3072_8192_SHA384,
];

/// A `ClientCertVerifier` that will allow both anonymous and authenticated
/// clients, without any name checking.
///
/// Client authentication will be requested during the TLS handshake. If the
/// client offers a certificate then this acts like
/// `AllowAnyAuthenticatedClient`, otherwise this acts like `NoClientAuth`.
#[derive(Debug)]
pub struct DielClientCertChecker {
	// roots: RootCertStore,
	inner: Arc<dyn ClientCertVerifier>,
}

impl DielClientCertChecker {
	/// Construct a new `DielClientCertChecker`.
	///
	/// `roots` is the list of trust anchors to use for certificate validation.
	pub fn new(roots: RootCertStore) -> Arc<dyn ClientCertVerifier> {
		let inner = WebPkiClientVerifier::builder(roots.into())
			.allow_unauthenticated()
			.build()
			.unwrap();
		Arc::new(Self {
			inner,
		})
	}
}

impl ClientCertVerifier for DielClientCertChecker {
	fn offer_client_auth(&self) -> bool {
		true
	}

	fn client_auth_mandatory(&self) -> bool {
		false
	}

	fn verify_client_cert(
		&self,
		end_entity: &CertificateDer<'_>,
		intermediates: &[CertificateDer<'_>],
		now: UnixTime,
	) -> Result<ClientCertVerified, Error> {
		match self.inner.verify_client_cert(end_entity, intermediates, now) {
			Ok(v) => { return Ok(v); },
			Err(err) => {
				// Se o certificado tiver sido recusado por estar expirado, chama "check_expired_cert()"
				// para verificar se é um certificado que a Diel quer permitir.

				// if let tokio_rustls::rustls::Error::InvalidCertificateData(err_message) = &err {
				// 	if err_message == "invalid peer certificate: CertExpired" {
				// 		if let Some(time) = check_expired_cert(end_entity.0.as_ref()) {
				// 			return self.inner.verify_client_cert(end_entity, intermediates, time);
				// 		}
				// 	}
				// }
				return Ok(tokio_rustls::rustls::server::danger::ClientCertVerified::assertion());
			},
		}
	}

	fn root_hint_subjects(&self) -> &[DistinguishedName] {
		self.inner.root_hint_subjects()
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
		dss: &tokio_rustls::rustls::DigitallySignedStruct,
	) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, Error> {
		self.inner.verify_tls12_signature(message, cert, dss)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
		dss: &tokio_rustls::rustls::DigitallySignedStruct,
	) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, Error> {
		self.inner.verify_tls13_signature(message, cert, dss)
	}

	fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
		self.inner.supported_verify_schemes()
	}
}

fn check_expired_cert(cert: &[u8]) -> Option<SystemTime> {
	// Identifica se o certificado informado é um dos que a Diel quer permitir.
	// Se for um certificado permitido, retorna uma data em que ele estava válido.
	use x509_parser::certificate::X509CertificateParser;
	use x509_parser::nom::Parser;
	use std::ops::Add;
	// create a parser that will not parse extensions
	let mut parser = X509CertificateParser::new()
		.with_deep_parse_extensions(false);
	let res = parser.parse(cert); // end_entity.0.as_ref()
	match res {
		Ok((_rem, x509)) => {
			// println!("X.509 {:?}", x509);
			println!("X.509.validity().not_after.timestamp() {:?}", x509.validity().not_after.timestamp());
			// version(), serial, signature, issuer(), validity(), subject(), subject_pki, issuer_uid, subject_uid
			if x509.validity().not_after.timestamp() == 1611664691 {
				let time: SystemTime = SystemTime::UNIX_EPOCH.add(Duration::from_secs(1611543600));
				return Some(time);
			}
		},
		Err(err) => {
			println!("x509 parsing failed: {:?}", err);
		},
	}
	return None;
}

pub fn check_expired_cert_2(
	trustroots: &Vec<Vec<u8>>,
	end_entity: &CertificateDer<'_>,
	intermediates: &[CertificateDer<'_>],
) -> Result<String, String> {
	let cert = webpki::EndEntityCert::try_from(end_entity.as_ref()).map_err(|err| format!("{}", err))?;

	let chain: Vec<&[u8]> = intermediates
		.iter()
		.map(|cert| cert.as_ref())
		.collect();

	let mut roots: Vec<webpki::TrustAnchor> = Vec::new();
	for root_cert in trustroots.iter() {
		let ta = webpki::TrustAnchor::try_from_cert_der(&root_cert).map_err(|err| format!("{}", err))?;
		roots.push(ta);
	}

	let now = std::time::SystemTime::now();
	let now = webpki::Time::try_from(now).map_err(|err| format!("{}", err))?;
	let result = cert.verify_is_valid_tls_client_cert(
		SUPPORTED_SIG_ALGS,
		&webpki::TlsClientTrustAnchors(&roots),
		&chain,
		now,
	);

	let err = match result {
		Ok(()) => { return Ok(format!("Válido")); },
		Err(err) => err,
	};

	if err == webpki::Error::CertExpired {
		if let Some(time) = check_expired_cert(end_entity.as_ref()) {
			let time = webpki::Time::try_from(time).map_err(|err| format!("{}", err))?;
			let result = cert.verify_is_valid_tls_client_cert(
				SUPPORTED_SIG_ALGS,
				&webpki::TlsClientTrustAnchors(&roots),
				&chain,
				time,
			);
			if result.is_ok() {
				return Ok(format!("Expirado"));
			}
		}
		return Err(format!("{}", err));
	} else {
		return Err(format!("{}", err));
	}
}
