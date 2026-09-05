use crate::engine::core::requested_history_ms;
use crate::{Metric, MetricDependency, MetricId, MetricSample};
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::debug;

/// Bounded time-ordered buffer of metric samples for one metric type.
///
/// Samples are kept sorted and deduplicated by timestamp. The buffer does not
/// enforce a retention window on its own; use
/// [`TimeSeriesBuffer::evict_samples_before_ts`] (or [`BufferStore::evict_before`])
/// to drop stale samples.
#[derive(Debug, Clone)]
pub struct TimeSeriesBuffer<T: Metric> {
    pub samples: VecDeque<MetricSample<T>>,
}

impl<T: Metric> Default for TimeSeriesBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Metric> TimeSeriesBuffer<T> {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::new(),
        }
    }

    /// Appends an already timestamped sample.
    pub fn push_sample(&mut self, sample: MetricSample<T>) {
        self.push_samples(vec![sample]);
    }

    /// Pushes samples, sorting and deduplicating by timestamp.
    pub fn push_samples(&mut self, historic: Vec<MetricSample<T>>) {
        let mut all: Vec<_> = self.samples.drain(..).chain(historic).collect();
        all.sort_by_key(|s| s.timestamp_ms);
        all.dedup_by_key(|s| s.timestamp_ms);
        self.samples = all.into_iter().collect();
    }

    /// Returns the newest sample, if one is available.
    pub fn get_sample_latest(&self) -> Option<&MetricSample<T>> {
        self.samples.back()
    }

    /// Returns the samples within the given timestamp window, sorted by
    /// timestamp. Returns an empty vec when the window is empty or invalid
    /// (`start_ts > end_ts`).
    pub fn get_samples_in_ts_window(&self, start_ts: i64, end_ts: i64) -> Vec<MetricSample<T>> {
        let start_idx = self.samples.partition_point(|s| s.timestamp_ms < start_ts);
        let end_idx = self.samples.partition_point(|s| s.timestamp_ms <= end_ts);
        self.samples
            .iter()
            .skip(start_idx)
            .take(end_idx.saturating_sub(start_idx))
            .cloned()
            .collect()
    }

    /// Returns the underlying deque for read-only iteration.
    pub fn get_samples(&self) -> &VecDeque<MetricSample<T>> {
        &self.samples
    }

    /// Return the last sample for or before the given timestamp, if any
    /// Returns the first sample with `ts <= timestamp` — the sample at the
    /// timestamp if one exists, otherwise the closest sample in the past.
    pub fn get_sample_last_before_ts(&self, timestamp_ms: i64) -> Option<&MetricSample<T>> {
        let index = self
            .samples
            .partition_point(|s| s.timestamp_ms <= timestamp_ms);
        self.samples.get(index.checked_sub(1)?)
    }

    /// Returns the sample with `timestamp - tolerance <= ts <= timestamp`
    /// (per the Metric's sample rate), if any.
    pub fn get_sample_for_ts(&self, timestamp_ms: i64) -> Option<&MetricSample<T>> {
        let tolerance_ms = T::sample_rate().tolerance_ms();
        let candidate = self.get_sample_last_before_ts(timestamp_ms)?;
        if candidate.timestamp_ms >= timestamp_ms.saturating_sub(tolerance_ms) {
            Some(candidate)
        } else {
            None
        }
    }

    /// Drops all samples older than `timestamp_ms`.
    pub fn evict_samples_before_ts(&mut self, timestamp_ms: i64) {
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.timestamp_ms < timestamp_ms)
        {
            self.samples.pop_front();
        }
    }

    /// Returns the number of samples currently retained.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Returns whether the buffer contains no samples.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

/// Type-erased operations over a [`TimeSeriesBuffer`], used by
/// [`BufferStore`] and [`TickLedger`] to interact with buffers of any metric
/// type without knowing `T`.
pub trait ErasedTimeSeriesBuffer: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn type_name(&self) -> &'static str;

    /// `(timestamp, debug-formatted value)` pairs for every retained sample.
    fn debug_rows(&self) -> Vec<(i64, String)>;

    /// Whether a sample exists within the metric's sample-rate tolerance of
    /// `timestamp_ms`.
    fn has_value_at(&self, timestamp_ms: i64) -> bool;

    /// Samples with `timestamp_ms > watermark_ts`, cloned and type-erased as
    /// `(timestamp, value)` pairs. Used by the persistence layer to extract
    /// unflushed samples without knowing `T`.
    fn samples_since(&self, watermark_ts: i64) -> Vec<(i64, Arc<dyn Any + Send + Sync>)>;

    /// `(timestamp, value)` of the newest retained sample, type-erased.
    fn latest_sample(&self) -> Option<TickSample>;

    /// Drops all samples older than `timestamp_ms`.
    fn evict_samples_before_ts(&mut self, timestamp_ms: i64);

    fn clone_erased(&self) -> Box<dyn ErasedTimeSeriesBuffer>;
}

impl<T: Metric> ErasedTimeSeriesBuffer for TimeSeriesBuffer<T> {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn debug_rows(&self) -> Vec<(i64, String)> {
        self.samples
            .iter()
            .map(|sample| (sample.timestamp_ms, format!("{:?}", sample.data)))
            .collect()
    }

    fn has_value_at(&self, timestamp_ms: i64) -> bool {
        self.get_sample_for_ts(timestamp_ms).is_some()
    }

    fn samples_since(&self, watermark_ts: i64) -> Vec<(i64, Arc<dyn Any + Send + Sync>)> {
        let start = self
            .samples
            .partition_point(|sample| sample.timestamp_ms <= watermark_ts);
        self.samples
            .iter()
            .skip(start)
            .map(|sample| {
                (
                    sample.timestamp_ms,
                    Arc::new(sample.data.clone()) as Arc<dyn Any + Send + Sync>,
                )
            })
            .collect()
    }

    fn latest_sample(&self) -> Option<TickSample> {
        self.samples.back().map(|sample| TickSample {
            timestamp_ms: sample.timestamp_ms,
            value: Arc::new(sample.data.clone()) as _,
            type_name: std::any::type_name::<T>(),
            debug: format!("{:?}", sample.data),
        })
    }

    fn evict_samples_before_ts(&mut self, timestamp_ms: i64) {
        TimeSeriesBuffer::evict_samples_before_ts(self, timestamp_ms)
    }

    fn clone_erased(&self) -> Box<dyn ErasedTimeSeriesBuffer> {
        Box::new(self.clone())
    }
}

/// A read-only immutable view of the buffer store to pass to Evaluators and out
/// of the engine.
pub struct ReadOnlyBufferStore<'a> {
    store: &'a BufferStore,
}
impl<'a> ReadOnlyBufferStore<'a> {
    #[inline]
    pub fn new(store: &'a BufferStore) -> Self {
        Self { store }
    }

    /// Direct typed accessor to concrete TimeSeriesBuffer<T>
    #[inline]
    pub fn get_buffer<T: Metric>(&self) -> Option<&'a TimeSeriesBuffer<T>> {
        self.store.get_buffer::<T>()
    }

    /// Whether the buffer for `metric_id` holds a sample within the metric's
    /// sample-rate tolerance of `timestamp_ms`.
    #[inline]
    pub fn has_value_at(&self, metric_id: MetricId, timestamp_ms: i64) -> bool {
        self.store
            .buffers
            .get(&metric_id)
            .is_some_and(|buffer| buffer.has_value_at(timestamp_ms))
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
    buffers: HashMap<MetricId, Box<dyn ErasedTimeSeriesBuffer>>,
}

impl Default for BufferStore {
    fn default() -> Self {
        Self::new()
    }
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
        let metric_id = MetricId::of::<T>();
        let requested_history_ms = requested_history_ms(demand, metric_id);
        let effective_buffer_size_ms = buffer_size_ms.max(requested_history_ms).max(1);

        debug!(
            metric_id = ?metric_id,
            buffer_size_ms = effective_buffer_size_ms,
            requested_history_ms,
            "registering metric buffer"
        );

        self.buffers
            .insert(metric_id, Box::new(TimeSeriesBuffer::<T>::new()));
    }

    #[inline]
    pub fn get_buffer<T: Metric>(&self) -> Option<&TimeSeriesBuffer<T>> {
        let metric_id = MetricId::of::<T>();
        self.buffers
            .get(&metric_id)?
            .as_ref()
            .as_any()
            .downcast_ref::<TimeSeriesBuffer<T>>()
    }

    #[inline]
    pub fn get_buffer_mut<T: Metric>(&mut self) -> Option<&mut TimeSeriesBuffer<T>> {
        let metric_id = MetricId::of::<T>();
        self.buffers
            .get_mut(&metric_id)?
            .as_any_mut()
            .downcast_mut::<TimeSeriesBuffer<T>>()
    }

    #[inline]
    pub fn push_sample<T: Metric>(&mut self, sample: MetricSample<T>) {
        let metric_id = MetricId::of::<T>();
        debug!(
            ?metric_id,
            timestamp_ms = sample.timestamp_ms,
            "committing metric sample to buffer"
        );
        match self.get_buffer_mut::<T>() {
            Some(buffer) => {
                buffer.push_sample(sample);
            }
            None => debug!(
                ?metric_id,
                "discarding sample because no metric buffer is registered"
            ),
        }
    }

    pub fn buffers_iter(
        &self,
    ) -> impl Iterator<Item = (&MetricId, &Box<dyn ErasedTimeSeriesBuffer>)> {
        self.buffers.iter()
    }

    /// Returns the samples newer than `watermark_ts` for a metric, cloned and
    /// type-erased for the persistence layer. Empty when the metric has no
    /// registered buffer.
    pub fn samples_since(
        &self,
        metric_id: MetricId,
        watermark_ts: i64,
    ) -> Vec<(i64, Arc<dyn Any + Send + Sync>)> {
        self.buffers
            .get(&metric_id)
            .map(|buffer| buffer.samples_since(watermark_ts))
            .unwrap_or_default()
    }

    /// Drops all samples older than `timestamp_ms` for a metric. No-op when
    /// the metric has no registered buffer.
    pub fn evict_before(&mut self, metric_id: MetricId, timestamp_ms: i64) {
        if let Some(buffer) = self.buffers.get_mut(&metric_id) {
            buffer.evict_samples_before_ts(timestamp_ms);
        }
    }

    /// Whether a buffer is registered for the metric type.
    pub fn has_buffer(&self, metric_id: MetricId) -> bool {
        self.buffers.contains_key(&metric_id)
    }

    /// Collects the newest sample of every buffered metric into a
    /// [`TickResult`], ready for cross-thread publication.
    pub fn tick_result(&self) -> TickResult {
        let mut values = HashMap::new();
        for (&metric_id, buffer) in self.buffers_iter() {
            if let Some(latest) = buffer.latest_sample() {
                values.insert(metric_id, latest);
            }
        }
        TickResult { values }
    }
}

/// A single type-erased sample from one tick result: the value for typed
/// downcast plus the concrete type's name and `Debug` rendering, captured at
/// build time so the result can be printed without knowing metric types.
pub struct TickSample {
    pub timestamp_ms: i64,
    value: Arc<dyn Any + Send + Sync>,
    pub type_name: &'static str,
    pub debug: String,
}

/// Tick-consistent result of one tick: the newest committed sample per
/// metric, published once per tick after all evaluators have fired.
///
/// Cheap to publish (one map build per tick, values shared via `Arc`) and
/// safe to hand across threads. Consumers downcast through
/// [`TickResult::get`]; for full time-series introspection use
/// [`TimeSeriesView`], built on demand instead of every tick.
#[derive(Default)]
pub struct TickResult {
    values: HashMap<MetricId, TickSample>,
}

impl TickResult {
    /// Returns the newest `(timestamp, value)` for a metric type.
    pub fn get<T: Metric>(&self) -> Option<(i64, Arc<T>)> {
        let sample = self.values.get(&MetricId::of::<T>())?;
        Some((
            sample.timestamp_ms,
            sample.value.clone().downcast::<T>().ok()?,
        ))
    }

    /// Whether the result holds any values at all.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Number of metrics present in the result.
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

/// One line per metric, sorted by metric type name so repeated tick results
/// diff cleanly.
impl std::fmt::Display for TickResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sections: Vec<&TickSample> = self.values.values().collect();
        sections.sort_by_key(|sample| sample.type_name);

        for sample in sections {
            writeln!(
                formatter,
                "  {} @ {}: {}",
                sample.type_name, sample.timestamp_ms, sample.debug
            )?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for TickResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TickResult({} metrics):\n{self}", self.len())
    }
}

/// Read view over the full retained time series of every buffered metric,
/// captured on demand (unlike the per-tick [`TickResult`]). Deep-copies the
/// buffers at capture time, so the view stays immutable and cross-thread
/// safe while the session keeps mutating its store.
///
/// `Box<dyn ErasedTimeSeriesBuffer>` is `Send + Sync` by trait bound and
/// metric values are `Send + Sync` via [`Metric`], so the view crosses
/// threads without unsafe impls.
pub struct TimeSeriesView {
    /// Timestamp the view was captured at.
    pub timestamp_ms: i64,
    buffers: HashMap<MetricId, Box<dyn ErasedTimeSeriesBuffer>>,
}

impl TimeSeriesView {
    /// Constructs an immutable, borrow-free view of the current BufferStore state.
    pub fn from_store(timestamp_ms: i64, store: &BufferStore) -> Self {
        let mut buffers = HashMap::default();

        for (&metric_id, buffer) in store.buffers_iter() {
            buffers.insert(metric_id, buffer.clone_erased());
        }

        Self {
            timestamp_ms,
            buffers,
        }
    }

    /// Read-only full-history accessor for one metric type.
    #[inline]
    pub fn get_buffer<T: Metric>(&self) -> Option<&TimeSeriesBuffer<T>> {
        let metric_id = MetricId::of::<T>();
        self.buffers
            .get(&metric_id)?
            .as_any()
            .downcast_ref::<TimeSeriesBuffer<T>>()
    }
}

/// Human-readable ledger dump: one section per buffered metric, samples in
/// chronological order. Section order is sorted by metric type name so
/// repeated views diff cleanly.
impl std::fmt::Display for TimeSeriesView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(formatter, "TimeSeriesView @ {}:", self.timestamp_ms)?;

        let mut sections: Vec<(&str, Vec<(i64, String)>)> = self
            .buffers
            .values()
            .map(|buffer| (buffer.type_name(), buffer.debug_rows()))
            .collect();
        sections.sort_by_key(|(type_name, _)| *type_name);

        for (type_name, rows) in sections {
            writeln!(formatter, "  {type_name} ({} samples):", rows.len())?;
            for (timestamp_ms, value) in rows {
                writeln!(formatter, "    {timestamp_ms}: {value}")?;
            }
        }
        Ok(())
    }
}
