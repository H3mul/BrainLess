pub mod backend;
pub mod duckdb_impl;

use crate::core::{
    Age, ErasedSeries, Metric, MetricDependency, MetricSample, SampleRate, SampleRequest,
    TickLedger, TimeSeriesBuffer,
};
pub use backend::{NoopStorageBackend, StorageBackend};
pub use duckdb_impl::DuckDbStorageBackend;
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info};

fn max_sample_rate(current: SampleRate, requested: SampleRate) -> SampleRate {
    match (current, requested) {
        (SampleRate::Best, _) | (_, SampleRate::Best) => SampleRate::Best,
        (SampleRate::Hz(current), SampleRate::Hz(requested)) => {
            SampleRate::Hz(current.max(requested))
        }
    }
}

/// Application-provided persistence mapping for a metric type.
///
/// The engine uses this abstraction to hand formatted rows to a storage
/// backend without knowing the database schema or SQL dialect.
pub trait PersistentMetric: Metric {
    /// Target table name for this metric model.
    fn table_name() -> &'static str;
    /// Column names passed to the storage backend.
    fn schema_columns() -> &'static [&'static str];
    /// DDL statement used to create the metric table.
    fn create_table_sql() -> &'static str;
    /// Parameterized insert statement used for batch flushing.
    fn insert_sql() -> &'static str;
    /// Query used to fetch a historic metric range.
    fn select_range_sql() -> &'static str;
    /// Formats metric values into backend row parameters.
    fn to_sql_params(&self) -> Vec<String>;
    /// Deserializes a backend row into the typed metric value.
    fn from_sql_row(row_params: &[&str]) -> Result<Self, String>;
    /// Target persistence rate for newly flushed samples. Defaults to 256 Hz.
    fn sample_rate() -> SampleRate {
        SampleRate::Hz(256)
    }
}

/// Type-erased buffer operations used by `StorageEngine`.
pub(crate) trait StorageBufferTrait: Send + Sync {
    fn clone_to_any(&self) -> Arc<dyn ErasedSeries>;

    fn commit_any(&mut self, sample: Box<dyn Any + Send + Sync>);

    fn flush_new_samples(
        &mut self,
        watermark: i64,
        backend: &mut dyn StorageBackend,
    ) -> Result<i64, String>;

    fn is_ephemeral(&self) -> bool;

    fn latest_erased(&self) -> Option<Box<dyn Any + Send + Sync>>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Bounded buffer for metrics that are flushed to the configured backend.
pub(crate) struct PersistentBuffer<T: PersistentMetric> {
    pub buffer: Arc<TimeSeriesBuffer<T>>,
}

impl<T: PersistentMetric> StorageBufferTrait for PersistentBuffer<T> {
    fn clone_to_any(&self) -> Arc<dyn ErasedSeries> {
        self.buffer.clone()
    }

    fn commit_any(&mut self, sample: Box<dyn Any + Send + Sync>) {
        if let Ok(sample) = sample.downcast::<MetricSample<T>>() {
            Arc::make_mut(&mut self.buffer).push_sample(*sample);
        }
    }

    fn flush_new_samples(
        &mut self,
        watermark: i64,
        backend: &mut dyn StorageBackend,
    ) -> Result<i64, String> {
        let samples: Vec<_> = self
            .buffer
            .as_slice()
            .iter()
            .filter(|s| s.timestamp_ms > watermark)
            .cloned()
            .collect();

        if samples.is_empty() {
            return Ok(watermark);
        }

        let rows: Vec<_> = samples
            .iter()
            .map(|s| format!("{},{}", s.timestamp_ms, s.data.to_sql_params().join(",")))
            .collect();

        backend.flush_batch(T::table_name(), T::schema_columns(), &rows)?;

        Ok(samples
            .iter()
            .map(|s| s.timestamp_ms)
            .max()
            .unwrap_or(watermark))
    }

    fn is_ephemeral(&self) -> bool {
        false
    }

    fn latest_erased(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.buffer.latest().cloned().map(|s| Box::new(s) as _)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Bounded buffer for derived metrics that are retained only in memory.
pub(crate) struct EphemeralBuffer<T: Metric> {
    pub buffer: Arc<TimeSeriesBuffer<T>>,
}

impl<T: Metric> StorageBufferTrait for EphemeralBuffer<T> {
    fn clone_to_any(&self) -> Arc<dyn ErasedSeries> {
        self.buffer.clone()
    }

    fn commit_any(&mut self, sample: Box<dyn Any + Send + Sync>) {
        if let Ok(sample) = sample.downcast::<MetricSample<T>>() {
            Arc::make_mut(&mut self.buffer).push_sample(*sample);
        }
    }

    fn flush_new_samples(
        &mut self,
        watermark: i64,
        _: &mut dyn StorageBackend,
    ) -> Result<i64, String> {
        Ok(watermark)
    }

    fn is_ephemeral(&self) -> bool {
        true
    }

    fn latest_erased(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.buffer.latest().cloned().map(|s| Box::new(s) as _)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Owns metric buffers, flush watermarks, and the configured persistence backend.
pub struct StorageEngine {
    buffers: HashMap<TypeId, Box<dyn StorageBufferTrait>>,
    flush_watermarks: HashMap<TypeId, i64>,
    last_flush_timestamp_ms: i64,
    flush_interval_ms: i64,
    backend: Box<dyn StorageBackend>,
}

impl StorageEngine {
    /// Creates storage with a periodic flush interval and backend.
    pub fn new(flush_interval_ms: i64, backend: Box<dyn StorageBackend>) -> Self {
        debug!(flush_interval_ms, "initializing metric storage engine");
        Self {
            buffers: HashMap::new(),
            flush_watermarks: HashMap::new(),
            last_flush_timestamp_ms: 0,
            flush_interval_ms,
            backend,
        }
    }

    /// Registers a persistent metric buffer.
    pub fn register_buffer<T: PersistentMetric>(
        &mut self,
        buffer_size_ms: i64,
        demand: &HashSet<MetricDependency>,
    ) -> Result<(), String> {
        let requested_history_ms = demand
            .iter()
            .filter(|dependency| dependency.type_id == TypeId::of::<T>())
            .map(|dependency| match &dependency.request {
                SampleRequest::Single(Age::SecondsAgo(seconds)) => *seconds as i64 * 1_000,
                SampleRequest::Window {
                    start: Age::SecondsAgo(seconds),
                    ..
                } => *seconds as i64 * 1_000,
                _ => 0,
            })
            .max()
            .unwrap_or(0);

        let requested_sample_rate = demand
            .iter()
            .filter(|dependency| dependency.type_id == TypeId::of::<T>())
            .filter_map(|dependency| match &dependency.request {
                SampleRequest::Window { rate, .. } => Some(rate.clone()),
                _ => None,
            })
            .fold(SampleRate::Hz(256), max_sample_rate);
        let effective_buffer_size_ms = buffer_size_ms.max(requested_history_ms).max(1);

        debug!(metric_type = ?TypeId::of::<T>(), buffer_size_ms = effective_buffer_size_ms, requested_history_ms, persistent = true, "registering metric buffer");

        self.buffers.insert(
            TypeId::of::<T>(),
            Box::new(PersistentBuffer {
                buffer: Arc::new(TimeSeriesBuffer::<T>::with_time_capacity_ms(
                    effective_buffer_size_ms,
                )),
            }),
        );

        self.flush_watermarks.insert(TypeId::of::<T>(), 0);

        if requested_history_ms > 0 {
            self.load_historic::<T>(requested_history_ms, requested_sample_rate)?;
        }
        Ok(())
    }

    /// Loads and decodes a historic range into an already registered persistent buffer.
    pub fn load_historic_range<T: PersistentMetric>(
        &mut self,
        start_ms: i64,
        end_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<(), String> {
        let rows =
            self.backend
                .fetch_historic_range(T::table_name(), start_ms, end_ms, sample_rate)?;
        self.load_historic_rows::<T>(rows)
    }

    /// Loads and decodes the most recent historic window into a persistent buffer.
    pub fn load_historic<T: PersistentMetric>(
        &mut self,
        window_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<(), String> {
        let rows = self
            .backend
            .fetch_historic(T::table_name(), window_ms, sample_rate)?;
        self.load_historic_rows::<T>(rows)
    }

    fn load_historic_rows<T: PersistentMetric>(&mut self, rows: Vec<String>) -> Result<(), String> {
        let buffer = self.buffers.get_mut(&TypeId::of::<T>()).ok_or_else(|| {
            format!(
                "Persistent buffer for {:?} is not registered",
                TypeId::of::<T>()
            )
        })?;
        let buffer = buffer
            .as_any_mut()
            .downcast_mut::<PersistentBuffer<T>>()
            .ok_or_else(|| {
                format!(
                    "Metric {:?} is not registered as persistent",
                    TypeId::of::<T>()
                )
            })?;
        let target = Arc::make_mut(&mut buffer.buffer);
        for row in rows {
            let fields: Vec<_> = row.split(',').collect();
            if fields.len() < 2 {
                continue;
            }
            let timestamp_ms = fields[0]
                .parse::<i64>()
                .map_err(|error| error.to_string())?;
            target.push_sample(MetricSample {
                timestamp_ms,
                data: T::from_sql_row(&fields[1..])?,
            });
        }
        Ok(())
    }

    /// Registers an in-memory-only metric buffer.
    pub fn register_ephemeral_buffer<T: Metric>(&mut self, buffer_size_ms: i64) {
        debug!(metric_type = ?TypeId::of::<T>(), buffer_size_ms, persistent = false, "registering metric buffer");
        self.buffers.insert(
            TypeId::of::<T>(),
            Box::new(EphemeralBuffer {
                buffer: Arc::new(TimeSeriesBuffer::<T>::with_time_capacity_ms(buffer_size_ms)),
            }),
        );

        self.flush_watermarks.insert(TypeId::of::<T>(), 0);
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

    /// Provisions the complete storage state for external consumption after a tick.
    pub fn provision_output_ledger(&self, timestamp_ms: i64) -> TickLedger {
        let mut ledger = TickLedger::new(timestamp_ms);
        for (type_id, buffer) in &self.buffers {
            ledger.insert_erased(*type_id, buffer.clone_to_any());
        }
        ledger
    }

    /// Commits a typed sample to its registered buffer.
    pub fn commit_sample<T: Metric>(&mut self, sample: MetricSample<T>) {
        let metric_type = TypeId::of::<T>();
        debug!(
            ?metric_type,
            timestamp_ms = sample.timestamp_ms,
            "committing metric sample to buffer"
        );
        if let Some(buf) = self.buffers.get_mut(&metric_type) {
            buf.commit_any(Box::new(sample));
        } else {
            debug!(
                ?metric_type,
                "discarding sample because no metric buffer is registered"
            );
        }
    }

    /// Clones a complete buffer for refreshing a tick ledger.
    #[allow(dead_code)]
    pub(crate) fn series_erased(&self, id: TypeId) -> Option<Arc<dyn ErasedSeries>> {
        self.buffers.get(&id).map(|b| b.clone_to_any())
    }

    /// Returns the newest sample for a type-erased metric.
    pub fn latest_erased(&self, id: TypeId) -> Option<Box<dyn Any + Send + Sync>> {
        self.buffers.get(&id).and_then(|b| b.latest_erased())
    }

    /// Flushes new persistent samples when the configured interval has elapsed.
    pub fn maybe_flush(&mut self, timestamp_ms: i64) -> Result<(), String> {
        if timestamp_ms - self.last_flush_timestamp_ms < self.flush_interval_ms {
            return Ok(());
        }

        info!(
            timestamp_ms,
            previous_flush_timestamp_ms = self.last_flush_timestamp_ms,
            buffer_count = self.buffers.len(),
            "flushing metric storage buffers"
        );
        let mut flushed_buffer_count = 0;
        for (id, buf) in self.buffers.iter_mut() {
            if !buf.is_ephemeral() {
                flushed_buffer_count += 1;
                let watermark = *self.flush_watermarks.get(id).unwrap_or(&0);

                let next = buf.flush_new_samples(watermark, self.backend.as_mut())?;

                self.flush_watermarks.insert(*id, next);
            }
        }

        self.last_flush_timestamp_ms = timestamp_ms;
        info!(flushed_buffer_count, "metric storage flush completed");

        Ok(())
    }
}
