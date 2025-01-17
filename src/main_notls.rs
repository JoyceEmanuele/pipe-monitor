mod configs;
mod mqttbytes;
mod client_cert_checker;
mod mode_generic_tunnel_notls;
mod tls_socket_rustls;
mod tcp_socket;
mod statistics;
mod mqtt_io;
mod lib_log;
mod lib_http {
    pub mod service;
    pub mod protocol;
    pub mod types;
    pub mod buffer;
    pub mod api;
    pub mod request;
    pub mod response;
}

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
    check_command_line_arguments();

    // Criar pasta de logs e já inserir um registro indicando que iniciou o serviço
    lib_log::create_log_dir().expect("Não foi possível criar a pasta de logs");
    lib_log::append_log_tag_msg("INIT", "Serviço iniciado");

    let configfile = crate::configs::load_configfile("./configfile-notls.json5".to_owned()).expect("configfile inválido");
    let (acceptor, ca_roots) = tls_socket_rustls::create_rustls_config(&configfile).expect("certificados inválidos");

    let num_brokers = configfile.BROKERS.len();
    let globs = Arc::new(GlobalVars {
        enable_debug: AtomicBool::new(true),
        stats_counters: statistics::StatisticsCounters::new(num_brokers),
        configfile,
        rustls_acceptor: acceptor,
        rustls_ca_roots: ca_roots,
    });

    let globs_clone = globs.clone();
    std::thread::spawn(move|| {
        statistics::thread_stats(globs_clone);
    });

    let bind_addr = globs.configfile.LISTEN_HTTP_API.to_owned();
    std::thread::spawn(move|| {
        println!("Starting HTTP server on {}", bind_addr);
        lib_http::service::run_service(&bind_addr);
    });

    mode_generic_tunnel_notls::run_server(globs);
}

fn check_command_line_arguments() {
    let args: Vec<String> = std::env::args().collect();
    if (args.len() >= 2) && (args[1] == "--test-config") {
        let path: String;
        if args.len() == 3 {
            path = args[2].to_owned();
        } else {
            path = crate::configs::default_configfile_path();
        }
        let result = crate::configs::load_configfile(path.clone());
        match result {
            Ok(_) => {
                println!("Arquivo de config [{:?}] OK!", path);
                std::process::exit(0);
            },
            Err(err) => {
                println!("Erro no arquivo de config: [{:?}]: {}", path, err);
                std::process::exit(1);
            },
        }
    }
}
