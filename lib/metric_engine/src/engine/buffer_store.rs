use crate::core::{
    requested_history_ms, ErasedSeries, Metric, MetricDependency, MetricSample, TickLedger,
    TimeSeriesBuffer,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// Type-erased in-memory buffer operations used by [`BufferStore`].
///
/// The store is storage agnostic: buffers hold samples, route commits, and
/// answer introspection queries. Persistence concerns (row encoding, SQL
/// parsing, flushing) live in [`crate::db::persistence`].
trait MetricBufferTrait: Send + Sync {
    fn clone_to_any(&self) -> Arc<dyn ErasedSeries>;

    fn commit_any(&mut self, sample: Box<dyn Any + Send + Sync>);

    fn latest_erased(&self) -> Option<Box<dyn Any + Send + Sync>>;

    /// Returns `(timestamp, data)` pairs for samples newer than `watermark`,
    /// ascending by timestamp. The data is the typed metric value, erased.
    fn samples_since(&self, watermark: i64) -> Vec<(i64, Box<dyn Any + Send + Sync>)>;

    /// Whether a sample exists within `tolerance_ms` of `timestamp_ms`.
    fn has_value_within(&self, timestamp_ms: i64, tolerance_ms: i64) -> bool;

    /// Drops all samples older than `timestamp_ms`.
    fn evict_before(&mut self, timestamp_ms: i64);
}

/// Bounded in-memory buffer for one metric type.
struct MetricBuffer<T: Metric> {
    buffer: Arc<TimeSeriesBuffer<T>>,
}

impl<T: Metric> MetricBufferTrait for MetricBuffer<T> {
    fn clone_to_any(&self) -> Arc<dyn ErasedSeries> {
        self.buffer.clone()
    }

    fn commit_any(&mut self, sample: Box<dyn Any + Send + Sync>) {
        if let Ok(sample) = sample.downcast::<MetricSample<T>>() {
            Arc::make_mut(&mut self.buffer).push_sample(*sample);
        }
    }

    fn latest_erased(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.buffer.latest().cloned().map(|s| Box::new(s) as _)
    }

    fn samples_since(&self, watermark: i64) -> Vec<(i64, Box<dyn Any + Send + Sync>)> {
        self.buffer
            .as_slice()
            .iter()
            .filter(|s| s.timestamp_ms > watermark)
            .map(|s| (s.timestamp_ms, Box::new(s.data.clone()) as _))
            .collect()
    }

    fn has_value_within(&self, timestamp_ms: i64, tolerance_ms: i64) -> bool {
        self.buffer
            .sample_within(timestamp_ms, tolerance_ms)
            .is_some()
    }

    fn evict_before(&mut self, timestamp_ms: i64) {
        Arc::make_mut(&mut self.buffer).evict_before(timestamp_ms);
    }
}

/// A collection of runtime in-memory metric sample buffers.
///
/// This is *not* persistent storage: every buffer lives and dies with the
/// session that owns the store. The store registers buffers per metric type,
/// routes committed samples to the right buffer, and answers introspection
/// queries (latest sample, sample presence, eviction). Persistent storage
/// interaction lives in [`crate::db::persistence`].
pub struct BufferStore {
    buffers: HashMap<TypeId, Box<dyn MetricBufferTrait>>,
}

impl BufferStore {
    pub fn new() -> Self {
        debug!("initializing metric buffer store");
        Self {
            buffers: HashMap::new(),
        }
    }

    /// Registers a buffer for a metric type, sized to hold at least the
    /// demanded history and the configured retention window.
    pub fn register_buffer<T: Metric>(
        &mut self,
        buffer_size_ms: i64,
        demand: &HashSet<MetricDependency>,
    ) {
        let metric_type = TypeId::of::<T>();
        let requested_history_ms = requested_history_ms(demand, metric_type);
        let effective_buffer_size_ms = buffer_size_ms.max(requested_history_ms).max(1);

        debug!(
            metric_type = ?metric_type,
            buffer_size_ms = effective_buffer_size_ms,
            requested_history_ms,
            "registering metric buffer"
        );

        self.buffers.insert(
            metric_type,
            Box::new(MetricBuffer::<T> {
                buffer: Arc::new(TimeSeriesBuffer::<T>::with_time_capacity_ms(
                    effective_buffer_size_ms,
                )),
            }),
        );
    }

    /// Commits a typed sample to its registered buffer.
    pub fn commit_sample<T: Metric>(&mut self, sample: MetricSample<T>) {
        let metric_type = TypeId::of::<T>();
        debug!(
            ?metric_type,
            timestamp_ms = sample.timestamp_ms,
            "committing metric sample to buffer"
        );
        self.commit_erased(metric_type, Box::new(sample));
    }

    /// Commits an already-boxed sample to the buffer registered for
    /// `type_id`.
    fn commit_erased(&mut self, type_id: TypeId, sample: Box<dyn Any + Send + Sync>) {
        if let Some(buf) = self.buffers.get_mut(&type_id) {
            buf.commit_any(sample);
        } else {
            debug!(
                ?type_id,
                "discarding sample because no metric buffer is registered"
            );
        }
    }

    /// Returns `(timestamp, data)` pairs for samples newer than `watermark`,
    /// ascending by timestamp. The data is the typed metric value, erased.
    /// Used by the persistence layer to drain flushable samples.
    pub(crate) fn samples_since(
        &self,
        type_id: TypeId,
        watermark: i64,
    ) -> Vec<(i64, Box<dyn Any + Send + Sync>)> {
        self.buffers
            .get(&type_id)
            .map(|buf| buf.samples_since(watermark))
            .unwrap_or_default()
    }

    /// Clones the requested buffers into a read-pass ledger.
    pub fn provision_ledger(
        &self,
        timestamp_ms: i64,
        demand: &HashSet<crate::core::MetricDependency>,
    ) -> TickLedger {
        debug!(
            timestamp_ms,
            dependency_count = demand.len(),
            "provisioning metric tick ledger"
        );
        let mut ledger = TickLedger::new(timestamp_ms);

        for dep in demand {
            if let Some(buf) = self.buffers.get(&dep.type_id) {
                ledger.insert_erased(dep.type_id, buf.clone_to_any());
            }
        }

        ledger
    }

    /// Provisions the complete buffer state for external consumption after a tick.
    pub fn provision_output_ledger(&self, timestamp_ms: i64) -> TickLedger {
        let mut ledger = TickLedger::new(timestamp_ms);
        for (type_id, buffer) in &self.buffers {
            ledger.insert_erased(*type_id, buffer.clone_to_any());
        }
        ledger
    }

    /// Returns the newest sample for a type-erased metric.
    pub fn latest_erased(&self, id: TypeId) -> Option<Box<dyn Any + Send + Sync>> {
        self.buffers.get(&id).and_then(|b| b.latest_erased())
    }

    /// Drops samples older than `timestamp_ms` from every buffer.
    pub fn evict_before(&mut self, timestamp_ms: i64) {
        debug!(timestamp_ms, "evicting samples from metric buffers");
        for buf in self.buffers.values_mut() {
            buf.evict_before(timestamp_ms);
        }
    }

    /// Whether the buffer for `T` holds a valid sample for `timestamp_ms`:
    /// a sample within the metric's target sample rate of the timestamp.
    /// If true, the metric does not need to be evaluated for this timestamp.
    pub fn has_value_at<T: Metric>(&self, timestamp_ms: i64) -> bool {
        self.has_value_within(
            TypeId::of::<T>(),
            timestamp_ms,
            T::sample_rate().tolerance_ms(),
        )
    }

    /// Type-erased variant of [`BufferStore::has_value_at`] with an explicit
    /// tolerance, for callers that only hold `TypeId`s.
    pub fn has_value_within(&self, type_id: TypeId, timestamp_ms: i64, tolerance_ms: i64) -> bool {
        self.buffers
            .get(&type_id)
            .is_some_and(|buf| buf.has_value_within(timestamp_ms, tolerance_ms))
    }
}