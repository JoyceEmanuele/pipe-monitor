mod configs;
mod mqttbytes;
mod client_cert_checker;
mod mode_generic_tunnel_devel;
mod tls_socket_rustls;
mod tcp_socket;
mod statistics_devel;
mod mqtt_io;

use mode_generic_tunnel_devel as mode_generic_tunnel;
use statistics_devel as statistics;
use std::sync::{atomic::AtomicBool, Arc};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub struct GlobalVars {
    pub enable_debug: AtomicBool,
    pub stats_counters: statistics::StatisticsCounters,
    pub configfile: configs::ConfigFile,
    pub rustls_acceptor: tokio_rustls::TlsAcceptor,
    pub rustls_ca_roots: Vec<Vec<u8>>,
}

fn main() {
    let configfile = crate::configs::load_default_configfile().expect("configfile inválido");

    if configfile.LISTEN_SOCKETS_DEVEL.len() != configfile.BROKERS.len() {
        println!("ERROR: invalid configuration: LISTEN_SOCKETS_DEVEL.len() != BROKERS.len()");
        std::thread::sleep(std::time::Duration::from_secs(3600));
        return;
    }

    let (acceptor, ca_roots) = tls_socket_rustls::create_rustls_config(&configfile).expect("certificados inválidos");
    let num_brokers = configfile.BROKERS.len();
    let globs = Arc::new(GlobalVars {
        enable_debug: AtomicBool::new(false),
        stats_counters: statistics::StatisticsCounters::new(num_brokers),
        configfile,
        rustls_acceptor: acceptor,
        rustls_ca_roots: ca_roots,
    });

    let globs_clone = globs.clone();
    std::thread::spawn(move|| {
        statistics::thread_stats(globs_clone);
    });

    mode_generic_tunnel::run_server(globs);
}
