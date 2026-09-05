use crate::engine::buffer_store::{BufferStore, MetricSnapshot};
use crate::execution::dependency_graph::DependencyGraph;
use crate::execution::tick_executor::TickExecutor;
use crate::{Metric, MetricEngine, MetricId, MetricSample};

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

/// Live-only configuration:
///   - metrics are fed externally, execution ticks are externally triggered
///   - data is retained in memory for calculation purposes and live feedback,
///     with periodic flushing to persistent storage
#[derive(Default)]
pub struct LiveSessionConfig {
    /// Total in-memory buffer size for all metrics, in milliseconds.
    pub buffer_size_ms: i64,
    /// Interval at which to flush buffers to storage, in milliseconds.
    pub flush_interval_ms: i64,
    /// Set of source metrics the session expects to receive externally.
    pub source_metrics: HashSet<MetricId>,
    /// Set of output metrics the session is expected to produce
    /// (Optional, if absent defaults to entire metric set)
    pub output_metrics: Option<HashSet<MetricId>>,
}

// Replay configuration contains only replay-specific settings.
// pub struct ReplaySessionConfig {
//     pub start_time_ms: i64,
//     pub end_time_ms: i64,
//     pub chunk_size_minutes: u64,
//     pub write_whitelist: Vec<MetricId>,
// }

#[allow(dead_code)]
pub struct LiveSession {
    config: LiveSessionConfig,
    store: BufferStore,
    executor: TickExecutor,
}

impl LiveSession {
    pub(crate) fn new(config: LiveSessionConfig, engine: &MetricEngine) -> Self {
        let dep_graph = DependencyGraph::new(engine.evaluators.clone())
            .expect("failed to build dependency graph");

        // If output_metrics is not specified, default to all metrics in the engine.
        let output_metrics = config
            .output_metrics
            .clone()
            .unwrap_or_else(|| engine.metrics.iter().map(|m| m.metric_id).collect());

        let plan = dep_graph
            .build_execution_plan(&config.source_metrics, &output_metrics)
            .expect("failed to build execution plan");

        Self {
            config,
            store: BufferStore::new(),
            executor: TickExecutor::new(plan),
        }
    }

    /// Add a metric to the store at the current timestamp.
    pub fn push_live_metric<T: Metric>(&mut self, data: T) {
        self.push_metric(current_timestamp(), data);
    }

    /// Add a metric to the store at the given timestamp.
    pub fn push_metric<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.store.push_sample(MetricSample { timestamp_ms, data });
    }

    /// Add a metric to the store at the current timestamp and tick the executor immediately.
    pub fn feed_live_metric<T: Metric>(&mut self, data: T) {
        self.push_live_metric(data);
        self.tick()
    }

    /// Add a batch of metrics to the store at the current timestamp and tick the executor immediately.
    pub fn feed_live_metric_batch<T: Metric>(&mut self, batch: Vec<T>) {
        for metric in batch {
            self.push_live_metric(metric);
        }
        self.tick()
    }

    /// Tick the executor at the current timestamp.
    pub fn tick(&mut self) {
        self.tick_at(current_timestamp())
    }

    /// Tick the executor at the given timestamp.
    fn tick_at(&mut self, timestamp_ms: i64) {
        self.executor.tick(timestamp_ms, &mut self.store);
    }

    /// Returns a point-in-time immutable snapshot for the session's latest evaluated timestamp.
    pub fn get_metric_snapshot(&self) -> MetricSnapshot {
        self.get_metric_snapshot_at(current_timestamp())
    }

    /// Returns a point-in-time immutable snapshot captured at a specific timestamp.
    pub fn get_metric_snapshot_at(&self, timestamp_ms: i64) -> MetricSnapshot {
        MetricSnapshot::from_store(timestamp_ms, &self.store)
    }
}

pub fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get current timestamp")
        .as_millis() as i64
}

// pub struct ReplaySession {
//     pub config: ReplaySessionConfig,
//     runtime: EngineRuntime,
// }
// impl ReplaySession {
//     pub(crate) fn new(
//         config: ReplaySessionConfig,
//         backend: Box<dyn StorageBackend>,
//         engine: &MetricEngine,
//     ) -> Self {
//         Self { config }
//     }
//     pub fn run(&mut self) -> Result<(), String> {
//         let _ = &self.runtime;
//         Ok(())
//     }
// }
