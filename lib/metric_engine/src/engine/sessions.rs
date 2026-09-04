use crate::core::{Metric, MetricSample, TickOutputLedger};
use crate::dag::DagGraphTraversal;
use crate::engine::buffer_store::BufferStore;
use crate::{DagGraph, ExecutionMode, MetricEngine};
use std::any::TypeId;
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
    pub source_metrics: HashSet<TypeId>,
    /// Set of output metrics the session is expected to produce
    /// (Optional, if absent defaults to entire metric set)
    pub output_metrics: Option<HashSet<TypeId>>,
}

/// Replay configuration contains only replay-specific settings.
pub struct ReplaySessionConfig {
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub chunk_size_minutes: u64,
    pub write_whitelist: Vec<TypeId>,
}

#[allow(dead_code)]
pub struct LiveSession {
    config: LiveSessionConfig,
    storage: BufferStore,
    dependency_traversal: DagGraphTraversal,
}

impl LiveSession {
    pub(crate) fn new(config: LiveSessionConfig, engine: &MetricEngine) -> Self {
        let graph = DagGraph::new(engine.evaluators.clone()).expect("failed to compile metric dag");

        // If output_metrics is not specified, default to all metrics in the engine.
        let output_metrics = config
            .output_metrics
            .clone()
            .unwrap_or_else(|| engine.metrics.iter().map(|m| m.metric_type).collect());

        let dependency_traversal = graph
            .traverse(
                &config.source_metrics,
                &output_metrics,
                ExecutionMode::Sequential,
            )
            .expect("failed to traverse metric dag");

        let mut storage = BufferStore::new();

        for metric in &engine.metrics {
            (metric.register)(
                &mut storage,
                config.buffer_size_ms,
                &dependency_traversal.aggregate_demand,
            )
            .expect("failed to register metric buffer");
        }

        Self {
            config,
            storage,
            dependency_traversal,
        }
    }
    pub fn push_live_metric<T: Metric>(&mut self, data: T) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_millis() as i64;
        self.push_metric(now_ms, data);
    }
    pub fn push_metric<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.storage
            .commit_sample(MetricSample { timestamp_ms, data });
    }
    pub fn tick(&mut self) -> Result<TickOutputLedger, String> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis() as i64;
        self.tick_at(now_ms)
    }

    pub fn feed_live_metric<T: Metric>(&mut self, data: T) -> Result<TickOutputLedger, String> {
        self.push_live_metric(data);
        self.tick()
    }

    pub fn feed_live_metric_batch<T: Metric>(
        &mut self,
        batch: Vec<T>,
    ) -> Result<TickOutputLedger, String> {
        for metric in batch {
            self.push_live_metric(metric);
        }
        self.tick()
    }
    fn tick_at(&mut self, timestamp_ms: i64) -> Result<TickOutputLedger, String> {
        self.dependency_traversal
            .execution_plan
            .execute(&mut self.storage, timestamp_ms)?;
        Ok(TickOutputLedger::new(
            self.storage.provision_output_ledger(timestamp_ms),
        ))
    }
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
