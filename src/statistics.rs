use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicIsize, Ordering};
use tokio::sync::Mutex;
use crate::{lib_log, GlobalVars};
use serde_json::json;
use std::str::FromStr;

#[derive(Debug,serde::Serialize,serde::Deserialize)]
pub struct ServiceStats {
	pub conn_arr: AtomicUsize,
	pub conn_closed: AtomicUsize,
	pub est_conn: AtomicIsize,
	pub tls_err: AtomicUsize,
	pub subscr: AtomicUsize,
	pub pub_brtodv: AtomicUsize,
	pub pub_others: AtomicUsize,
	pub pub_dup: AtomicUsize,
	pub topic_data: AtomicUsize,
	pub topic_ctrl: AtomicUsize,
	pub topic_cmd: AtomicUsize,
}

impl ServiceStats {
	pub fn new() -> Self {
		ServiceStats {
			conn_arr: AtomicUsize::new(0),
			conn_closed: AtomicUsize::new(0),
			est_conn: AtomicIsize::new(0),
			tls_err: AtomicUsize::new(0),
			subscr: AtomicUsize::new(0),
			pub_brtodv: AtomicUsize::new(0),
			pub_others: AtomicUsize::new(0),
			pub_dup: AtomicUsize::new(0),
			topic_data: AtomicUsize::new(0),
			topic_ctrl: AtomicUsize::new(0),
			topic_cmd: AtomicUsize::new(0),
		}
	}
}

pub struct StatisticsCounters {
	// pub mqtt_conn: AtomicUsize,
	pub est_conn: AtomicIsize,
	pub b_stats: Vec<ServiceStats>,
	pub devs_conn: Mutex<HashMap<String,isize>>,
}

impl StatisticsCounters {
	pub fn new(num_brokers: usize) -> Self {
		let mut b_stats: Vec<ServiceStats> = Vec::new();
		for i in 0..num_brokers {
			b_stats.push(ServiceStats::new());
		}
		StatisticsCounters {
			est_conn: AtomicIsize::new(0),
			b_stats,
			devs_conn: Mutex::new(HashMap::new()),
		}
	}
}

pub fn thread_stats(globs: Arc<GlobalVars>) {
	use std::ops::Sub;
	use std::io::Write;
	let mut runtime = tokio::runtime::Builder::new_current_thread();
	let runtime = runtime.enable_all().build().expect("Não foi possível criar o runtime tokio");
	runtime.block_on(async {
		const INTERVAL: u64 = 60; // em segundos
		let mut ts_start = std::time::Instant::now();

		loop {
			tokio::time::sleep(std::time::Duration::from_secs(INTERVAL)).await;
			let elapsed = ts_start.elapsed().as_secs();
			ts_start = std::time::Instant::now();

			let mut message = serde_json::json!({
				"origin": "broker-diel-v1",
				"interval": elapsed,
				"est_conn": globs.stats_counters.est_conn.load(Ordering::Relaxed),
				"brokers_stats": [],
			});
			let brokers_stats = message["brokers_stats"].as_array_mut().expect("Erro ao pegar brokers_stats");
			for b_stats in &globs.stats_counters.b_stats {
				brokers_stats.push(serde_json::json!({
					"conn_arr": get_reset_atomic_usize(&b_stats.conn_arr),
					"conn_closed": get_reset_atomic_usize(&b_stats.conn_closed),
					"est_conn": b_stats.est_conn.load(Ordering::Relaxed),
					"tls_err": get_reset_atomic_usize(&b_stats.tls_err),
					"subscr": get_reset_atomic_usize(&b_stats.subscr),
					"pub_brtodv": get_reset_atomic_usize(&b_stats.pub_brtodv),
					"pub_others": get_reset_atomic_usize(&b_stats.pub_others),
					"pub_dup": get_reset_atomic_usize(&b_stats.pub_dup),
					"topic_data": get_reset_atomic_usize(&b_stats.topic_data),
					"topic_ctrl": get_reset_atomic_usize(&b_stats.topic_ctrl),
					"topic_cmd": get_reset_atomic_usize(&b_stats.topic_cmd),
				}));
			}
			let payload_light = message.to_string();

			let payload_full = {
				let mut devs_conn = globs.stats_counters.devs_conn.lock().await;
				message["devs_conn"] = serde_json::json!(*devs_conn);
				let payload_full = message.to_string();
				devs_conn.clear();
				payload_full
			};

			let globs = globs.clone();
			tokio::spawn(async move {
				println!("Thread stats: {}", payload_light);
				{
					let now = chrono::Utc::now().sub(chrono::Duration::hours(3));
					let result = std::fs::OpenOptions::new()
						.write(true)
						.create(true)
						.append(true)
						.open(lib_log::stats_file_name_for_day(&now.to_rfc3339()[0..10]))
						.and_then(|mut file| {
							file.write_all(format!("{:?}-0300 ", &now.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[0..19]).as_bytes())?;
							file.write_all(payload_full.as_bytes())?;
							file.write_all(b"\n")
						});
					if let Err(err) = result {
						println!("Error writing to file: {}", err);
					}
				}
				if let Err(err) = announce_server_port(globs).await {
					println!("Error announcing server: {}", err);
				}
			});
		}
	});
}

fn get_reset_atomic_usize(atomic_value: &AtomicUsize) -> usize {
	let value = atomic_value.load(Ordering::Relaxed);
	atomic_value.fetch_sub(value, Ordering::Relaxed);
	value
}

async fn announce_server_port(globs: Arc<GlobalVars>) -> Result<(), String> {
	let bind_addr = u16::from_str(globs.configfile.LISTEN_HTTP_API.split(':').last().expect("IMPOSS-132")).expect("IMPOSS-133");
	let body = json!({
    "report": { "origin": "broker-diel-v1" },
    "apiPort": bind_addr,
    "hostname": std::env::var("HOSTNAME").ok(),
	});

	let stats_url = format!("http://{}/diel-internal/realtime/setServerStatus_brokersGateway", globs.configfile.STATS_SERVER_HTTP);
	let client = reqwest::Client::new();
	let res = client.post(&stats_url)
		.json(&body)
		.send()
		.await
		.map_err(|e| e.to_string())?;
	let response_status = res.status();

	if response_status != reqwest::StatusCode::OK {
		return Err(format!("Invalid announce response: {} {}", stats_url, response_status));
	}

	Ok(())
}
