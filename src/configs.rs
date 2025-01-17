use json5;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ConfigFile {
  pub LISTEN_SOCKET: String, // "0.0.0.0:8883"
  pub BROKERS: Vec<HostPort>, // [ { host: "broker1.lan.dielenergia.com", port: 1883 } ]
  pub LISTEN_SOCKETS_DEVEL: Vec<String>, // [ "0.0.0.0:18001" ]
  pub LISTEN_HTTP_API: String, // "0.0.0.0:46882";
  pub STATS_SERVER_HTTP: String, // "api.lan.dielenergia.com:46101";
  pub CERT_PATH: String, // "/path/to/public_cert_broker-diel.pem";
  pub KEY_PATH: String, // "/path/to/private_key_broker-diel.pem";
  pub CA_PATH: String, // "/path/to/ca_public_cert_diel_v2.pem";
}

#[derive(Deserialize, Debug)]
pub struct HostPort {
  pub host: String,
  pub port: u16,
}

pub fn default_configfile_path() -> String {
  "./configfile.json5".to_owned()
}

pub fn load_default_configfile() -> Result<ConfigFile, String> {
  let default_path = default_configfile_path();
  let default_missing = !std::path::Path::new(&default_path).exists();
  if default_missing {
    let example_config = include_str!("../configfile_example.json5");
    let res = json5::from_str(&example_config);
    if res.is_ok() {
      println!("Nenhum arquivo de configuração encontrado, usando configuração de exemplo");
      return res.map_err(|err| format!("{}", err));
    }
  }
  load_configfile(default_path)
}

pub fn load_configfile(path: String) -> Result<ConfigFile, String> {
  let file_contents = std::fs::read_to_string(&path).map_err(|err| err.to_string())?;
  json5::from_str(&file_contents).map_err(|err| err.to_string())
}
