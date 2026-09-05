use crate::db::backend::StorageBackend;
use crate::db::persistence::PersistenceDriver;
use crate::engine::buffer_store::{BufferStore, TickResult, TimeSeriesView};
use crate::engine::core::requested_history_ms;
use crate::execution::dependency_graph::DependencyGraph;
use crate::execution::tick_executor::TickExecutor;
use crate::{Metric, MetricEngine, MetricId, MetricSample};

use arc_swap::ArcSwap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Live-only configuration:
///   - metrics are fed externally, execution ticks are externally triggered
///   - data is retained in memory for calculation purposes and live feedback,
///     with periodic flushing to persistent storage
#[derive(Default)]
pub struct LiveSessionConfig {
    /// Total in-memory buffer size for all metrics, in milliseconds.
    pub buffer_size_ms: i64,
    /// Interval at which to flush buffers to storage, in milliseconds.
    /// Checked on every tick; zero flushes on every tick.
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
    persistence: PersistenceDriver,
    /// Result of the most recent tick: newest committed sample per metric,
    /// published once per tick after all evaluators have fired, so
    /// cross-thread readers always observe a tick-consistent view.
    last_tick: ArcSwap<TickResult>,
    /// Per-metric in-memory retention window in milliseconds. Samples older
    /// than the tick timestamp minus this window are evicted on every tick.
    /// Floor is twice the flush interval so unflushed
    /// samples always survive at least one full flush opportunity; raised to
    /// the configured buffer size and any evaluator-demanded history.
    retention_ms: HashMap<MetricId, i64>,
}

impl LiveSession {
    pub(crate) fn new(
        config: LiveSessionConfig,
        engine: &MetricEngine,
        backend: Box<dyn StorageBackend>,
    ) -> Result<Self, String> {
        let dep_graph = DependencyGraph::new(engine.evaluators.clone())
            .map_err(|error| format!("failed to build dependency graph: {error}"))?;

        // If output_metrics is not specified, default to all metrics in the engine.
        let output_metrics = config
            .output_metrics
            .clone()
            .unwrap_or_else(|| engine.metrics.iter().map(|m| m.metric_id).collect());

        let plan = dep_graph
            .build_execution_plan(&config.source_metrics, &output_metrics)
            .map_err(|error| format!("failed to build execution plan: {error}"))?;

        let mut store = BufferStore::new();
        let mut persistence = PersistenceDriver::new(config.flush_interval_ms, backend);

        // Register a buffer for every metric the plan touches: externally fed
        // sources plus all metrics produced by the planned evaluators.
        // Persistent registrations also record their row codecs with the
        // driver.
        for registration in engine
            .metrics
            .iter()
            .filter(|registration| plan.plan_metrics.contains(&registration.metric_id))
        {
            (registration.register)(
                &mut store,
                &mut persistence,
                config.buffer_size_ms,
                &plan.aggregate_demand,
            )?;
        }

        for metric_id in &plan.plan_metrics {
            if !store.has_buffer(*metric_id) {
                warn!(
                    ?metric_id,
                    "session metric has no engine registration; no buffer created"
                );
            }
        }

        // Retention floor of twice the flush interval guarantees a sample
        // stays in memory across one missed flush opportunity before it can
        // be evicted.
        let flush_safety_ms = config.flush_interval_ms.saturating_mul(2);
        let retention_ms = plan
            .plan_metrics
            .iter()
            .map(|&metric_id| {
                let retention = flush_safety_ms
                    .max(config.buffer_size_ms)
                    .max(requested_history_ms(&plan.aggregate_demand, metric_id));
                (metric_id, retention)
            })
            .collect();

        Ok(Self {
            config,
            store,
            executor: TickExecutor::new(plan),
            persistence,
            last_tick: ArcSwap::from_pointee(TickResult::default()),
            retention_ms,
        })
    }

    /// Add a metric to the store at the given timestamp. Afterwards flushes
    /// (if the interval elapsed) and evicts samples outside the retention
    /// window, measured from the pushed timestamp.
    pub fn push_metric<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.store.push_sample(MetricSample { timestamp_ms, data });
    }

    /// Add a batch of metrics to the store at the current timestamp and tick the executor immediately.
    pub fn push_metric_batch<T: Metric>(&mut self, timestamp_ms: i64, batch: Vec<T>) {
        for metric in batch {
            self.store.push_sample(MetricSample {
                timestamp_ms,
                data: metric,
            });
        }
    }

    /// Tick the executor at the given timestamp, then flush buffers to the
    /// persistent backend (if the flush interval elapsed) and evict samples
    /// outside the retention window. Once all evaluators have fired and
    /// maintenance ran, publishes a fresh [`TickResult`] and returns it: the
    /// pointer is cheap to hand to other threads, and readers can also load
    /// the newest publication at any time via [`LiveSession::last_tick`].
    /// Flush failures are logged and retried on a later flush; watermarks
    /// stay untouched so no samples are lost.
    pub fn tick(&mut self, timestamp_ms: i64) -> Arc<TickResult> {
        self.executor.tick(timestamp_ms, &mut self.store);
        self.maintain(timestamp_ms);
        self.publish_tick_result()
    }

    /// Returns the most recently published [`TickResult`] at any time,
    /// without mutating session state. The returned pointer stays valid and
    /// immutable even while later ticks publish fresher results.
    pub fn last_tick(&self) -> Arc<TickResult> {
        self.last_tick.load_full()
    }

    /// Builds the newest-per-metric result and publishes it atomically.
    fn publish_tick_result(&self) -> Arc<TickResult> {
        let result = Arc::new(self.store.tick_result());
        self.last_tick.store(Arc::clone(&result));
        result
    }

    /// Flushes buffered samples to the persistent backend when due, then
    /// evicts samples that fell outside their metric's retention window.
    /// Flushing runs before eviction so nothing is dropped before it had at
    /// least one flush opportunity.
    fn maintain(&mut self, now_ts: i64) {
        if let Err(error) = self.persistence.maybe_flush(&mut self.store, now_ts) {
            warn!(%error, timestamp_ms = now_ts, "periodic flush failed; will retry");
        }
        for (&metric_id, &retention) in &self.retention_ms {
            self.store
                .evict_before(metric_id, now_ts.saturating_sub(retention));
        }
    }

    /// Flushes all unflushed samples to the persistent backend immediately
    /// (e.g. on shutdown), regardless of the flush interval. Returns the
    /// number of metric buffers flushed.
    pub fn flush(&mut self) -> Result<usize, String> {
        self.persistence.flush(&mut self.store, current_timestamp())
    }

    /// Captures a [`TimeSeriesView`] of every buffered metric's full retained
    /// history on demand. Unlike the per-tick [`TickResult`], this deep-copies
    /// the buffers at call time, so it is not free — but the returned view is
    /// immutable and cross-thread safe while the session keeps mutating.
    pub fn time_series(&self) -> Arc<TimeSeriesView> {
        Arc::new(TimeSeriesView::from_store(current_timestamp(), &self.store))
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

#[cfg(test)]
mod tests;
