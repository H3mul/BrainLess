use crate::db::backend::StorageBackend;
use crate::engine::buffer_store::BufferStore;
use crate::engine::core::{Metric, MetricId};

use std::any::Any;
use std::collections::HashMap;
use tracing::{debug, info};

/// Application-provided persistence mapping for a metric type.
///
/// The persistence layer uses this abstraction to hand formatted rows to a
/// storage backend without knowing the database schema or SQL dialect.
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
}

/// Type-erased row encoder for one persistent metric type, registered at
/// metric registration time so the flush loop can encode drained samples
/// without knowing their concrete types.
pub(crate) struct ErasedPersistentMetric {
    pub table: &'static str,
    pub columns: &'static [&'static str],
    /// Encodes `(timestamp, data)` into backend row parameters; the data is
    /// the typed metric value, erased. `None` when the type does not match.
    pub encode_row: fn(i64, &(dyn Any + Send + Sync)) -> Option<Vec<String>>,
}

impl ErasedPersistentMetric {
    pub(crate) fn of<T: PersistentMetric>() -> Self {
        Self {
            table: T::table_name(),
            columns: T::schema_columns(),
            encode_row: encode_sample::<T>,
        }
    }
}

fn encode_sample<T: PersistentMetric>(
    timestamp_ms: i64,
    data: &(dyn Any + Send + Sync),
) -> Option<Vec<String>> {
    let data = data.downcast_ref::<T>()?;
    let mut params = Vec::with_capacity(1 + data.to_sql_params().len());
    params.push(timestamp_ms.to_string());
    params.extend(data.to_sql_params());
    Some(params)
}

/// Drives persistence for the buffer store: encodes buffered samples into
/// rows and flushes them to the backend on a schedule.
///
/// The buffer store stays storage agnostic; this type owns the backend, the
/// flush watermarks, and the per-type row persistent metrics.
pub struct PersistenceDriver {
    backend: Box<dyn StorageBackend>,
    persistent_metrics: HashMap<MetricId, ErasedPersistentMetric>,
    flush_watermarks: HashMap<MetricId, i64>,
    last_flush_timestamp_ms: i64,
    flush_interval_ms: i64,
}

impl PersistenceDriver {
    pub fn new(flush_interval_ms: i64, backend: Box<dyn StorageBackend>) -> Self {
        debug!(flush_interval_ms, "initializing persistence driver");
        Self {
            backend,
            persistent_metrics: HashMap::new(),
            flush_watermarks: HashMap::new(),
            last_flush_timestamp_ms: 0,
            flush_interval_ms,
        }
    }

    pub fn flush_interval_ms(&self) -> i64 {
        self.flush_interval_ms
    }

    /// Records the row persistent metric for a persistent metric type so its buffered
    /// samples can be encoded and flushed to the backend.
    pub fn register_metric<T: PersistentMetric>(&mut self) {
        let metric_id = MetricId::of::<T>();
        debug!(metric_id = ?metric_id, "registering persistent metric");
        self.persistent_metrics
            .insert(metric_id, ErasedPersistentMetric::of::<T>());
    }

    /// Flushes new samples to the backend only when the configured interval
    /// has elapsed since the last flush, measured on the provided timestamp.
    /// Returns the number of metric buffers flushed.
    pub fn maybe_flush(
        &mut self,
        store: &mut BufferStore,
        timestamp_ms: i64,
    ) -> Result<usize, String> {
        if timestamp_ms.saturating_sub(self.last_flush_timestamp_ms) < self.flush_interval_ms {
            return Ok(0);
        }
        self.flush(store, timestamp_ms)
    }

    /// Flushes every sample newer than its metric's watermark to the backend,
    /// regardless of the flush interval. Returns the number of metric buffers
    /// flushed. On backend failure the affected watermarks stay untouched, so
    /// the samples are retried by the next flush.
    pub fn flush(&mut self, store: &mut BufferStore, timestamp_ms: i64) -> Result<usize, String> {
        if self.backend.is_noop() {
            return Ok(0);
        }

        info!(
            timestamp_ms,
            previous_flush_timestamp_ms = self.last_flush_timestamp_ms,
            metric_count = self.persistent_metrics.len(),
            "flushing metric buffers to persistent storage"
        );
        let mut flushed_buffer_count = 0;
        for (metric_id, persistent_metric) in &self.persistent_metrics {
            let watermark = *self.flush_watermarks.get(metric_id).unwrap_or(&0);
            let samples = store.samples_since(*metric_id, watermark);
            if samples.is_empty() {
                continue;
            }

            let rows: Vec<Vec<String>> = samples
                .iter()
                .filter_map(|(timestamp, data)| {
                    (persistent_metric.encode_row)(*timestamp, data.as_ref())
                })
                .collect();
            if rows.is_empty() {
                continue;
            }

            let high_watermark = samples
                .iter()
                .map(|(timestamp, _)| *timestamp)
                .max()
                .unwrap_or(watermark);

            self.backend
                .flush_batch(persistent_metric.table, persistent_metric.columns, &rows)?;
            self.flush_watermarks.insert(*metric_id, high_watermark);
            flushed_buffer_count += 1;
        }

        self.last_flush_timestamp_ms = timestamp_ms;
        info!(flushed_buffer_count, "metric storage flush completed");

        Ok(flushed_buffer_count)
    }
}

#[cfg(test)]
mod tests;
