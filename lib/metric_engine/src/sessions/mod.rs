use crate::core::{Metric, MetricSample, TickOutputLedger};
use crate::engine::EngineRuntime;
use std::any::TypeId;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

/// Live-only configuration. Storage is supplied separately when creating the session.
#[derive(Default)]
pub struct LiveSessionConfig {
    pub buffer_size_ms: i64,
    pub flush_interval_ms: i64,
    pub source_metrics: HashSet<TypeId>,
    pub output_metrics: Option<HashSet<TypeId>>,
}

/// Replay configuration contains only replay-specific settings.
pub struct ReplaySessionConfig {
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub chunk_size_minutes: u64,
    pub write_whitelist: Vec<TypeId>,
}

pub struct LiveSession {
    pub config: LiveSessionConfig,
    runtime: EngineRuntime,
}
impl LiveSession {
    pub(crate) fn new(config: LiveSessionConfig, runtime: EngineRuntime) -> Self {
        Self { config, runtime }
    }
    pub fn push_live_metric<T: Metric>(&mut self, data: T) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_millis() as i64;
        self.push_metric(now_ms, data);
    }
    pub fn push_metric<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.runtime
            .storage
            .commit_sample(MetricSample { timestamp_ms, data });
    }
    pub fn tick(&mut self) -> Result<TickOutputLedger, String> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis() as i64;
        self.tick_at(now_ms)
    }
    pub fn feed_live_batch<T: Metric>(
        &mut self,
        batch: Vec<T>,
    ) -> Result<TickOutputLedger, String> {
        for metric in batch {
            self.push_live_metric(metric);
        }
        self.tick()
    }
    fn tick_at(&mut self, timestamp_ms: i64) -> Result<TickOutputLedger, String> {
        self.runtime
            .resources
            .execution_plan
            .execute(&mut self.runtime.storage, timestamp_ms)?;
        self.runtime.storage.maybe_flush(timestamp_ms)?;
        Ok(TickOutputLedger::new(
            self.runtime.storage.provision_output_ledger(timestamp_ms),
        ))
    }
}

pub struct ReplaySession {
    pub config: ReplaySessionConfig,
    runtime: EngineRuntime,
}
impl ReplaySession {
    pub(crate) fn new(config: ReplaySessionConfig, runtime: EngineRuntime) -> Self {
        Self { config, runtime }
    }
    pub fn run(&mut self) -> Result<(), String> {
        let _ = &self.runtime;
        Ok(())
    }
}
