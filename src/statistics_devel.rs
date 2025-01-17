use std::sync::{atomic::AtomicUsize, Arc};
use std::sync::atomic::Ordering;
use crate::GlobalVars;

pub struct ServiceStats {
	pub tls_err: AtomicUsize,
}

impl ServiceStats {
	pub fn new() -> Self {
		ServiceStats {
			tls_err: AtomicUsize::new(0),
		}
	}
}

pub struct StatisticsCounters {
	pub b_stats: Vec<ServiceStats>,
}

impl StatisticsCounters {
	pub fn new(num_brokers: usize) -> Self {
		let mut b_stats: Vec<ServiceStats> = Vec::new();
		for i in 0..num_brokers {
			b_stats.push(ServiceStats::new());
		}
		StatisticsCounters {
			b_stats,
		}
	}
}

pub fn thread_stats(globs: Arc<GlobalVars>) {
	let mut runtime = tokio::runtime::Builder::new_current_thread();
	let runtime = runtime.enable_all().build().expect("Não foi possível criar o runtime tokio");
	runtime.block_on(async {
		loop {
			tokio::time::sleep(std::time::Duration::from_secs(600)).await;
			for b_stats in &globs.stats_counters.b_stats {
				// Ignore on devel
				b_stats.tls_err.store(0, Ordering::Relaxed)
			}
		}
	});
}
