use crate::db::backend::StorageBackend;
use crate::engine::buffer_store::BufferStore;
use crate::engine::core::{
    Metric, MetricDependency, MetricId, MetricSample, SampleRate, requested_history_ms,
    requested_sample_rate,
};

use std::any::Any;
use std::collections::{HashMap, HashSet};
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
pub(crate) struct SampleCodec {
    pub table: &'static str,
    pub columns: &'static [&'static str],
    /// Encodes `(timestamp, data)` into backend row parameters; the data is
    /// the typed metric value, erased. `None` when the type does not match.
    pub encode_row: fn(i64, &(dyn Any + Send + Sync)) -> Option<Vec<String>>,
}

impl SampleCodec {
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

/// Drives persistence for the buffer manager: flushes drained samples to the
/// backend on a schedule and loads historic samples back into buffers.
///
/// The buffer manager stays storage agnostic; this type owns the flush
/// watermarks, the per-type row codecs, and the backend interaction.
pub struct PersistenceDriver {
    codecs: HashMap<MetricId, SampleCodec>,
    flush_watermarks: HashMap<MetricId, i64>,
    last_flush_timestamp_ms: i64,
    flush_interval_ms: i64,
}

impl PersistenceDriver {
    pub fn new(flush_interval_ms: i64) -> Self {
        debug!(flush_interval_ms, "initializing persistence driver");
        Self {
            codecs: HashMap::new(),
            flush_watermarks: HashMap::new(),
            last_flush_timestamp_ms: 0,
            flush_interval_ms,
        }
    }

    pub fn flush_interval_ms(&self) -> i64 {
        self.flush_interval_ms
    }

    /// Registers a persistent metric: creates its buffer (sized to also
    /// survive one flush interval), records its row codec, and backfills
    /// demanded history from the backend.
    pub fn register_persistent<T: PersistentMetric>(
        &mut self,
        store: &mut BufferStore,
        buffer_size_ms: i64,
        demand: &HashSet<MetricDependency>,
        backend: &dyn StorageBackend,
    ) -> Result<(), String> {
        let metric_id = MetricId::of::<T>();
        let effective_buffer_size_ms = buffer_size_ms.max(self.flush_interval_ms);

        debug!(
            metric_id = ?metric_id,
            buffer_size_ms = effective_buffer_size_ms,
            persistent = true,
            "registering persistent metric buffer"
        );

        store.register_buffer::<T>(effective_buffer_size_ms, demand);
        self.codecs.insert(metric_id, SampleCodec::of::<T>());
        self.flush_watermarks.entry(metric_id).or_insert(0);

        let requested_history_ms = requested_history_ms(demand, metric_id);
        if requested_history_ms > 0 {
            let sample_rate = requested_sample_rate(demand, metric_id);
            self.load_historic::<T>(store, requested_history_ms, sample_rate, backend)?;
        }
        Ok(())
    }

    /// Loads and decodes the most recent historic window into the buffer.
    pub fn load_historic<T: PersistentMetric>(
        &mut self,
        store: &mut BufferStore,
        window_ms: i64,
        sample_rate: SampleRate,
        backend: &dyn StorageBackend,
    ) -> Result<(), String> {
        debug!(
            metric_id = ?MetricId::of::<T>(),
            window_ms,
            ?sample_rate,
            "loading historic metric window"
        );
        let rows = backend.fetch_historic(T::table_name(), window_ms, sample_rate)?;
        self.load_rows::<T>(store, rows)
    }

    /// Loads and decodes a historic range into the buffer.
    pub fn load_historic_range<T: PersistentMetric>(
        &mut self,
        store: &mut BufferStore,
        start_ms: i64,
        end_ms: i64,
        sample_rate: SampleRate,
        backend: &dyn StorageBackend,
    ) -> Result<(), String> {
        debug!(
            metric_id = ?MetricId::of::<T>(),
            start_ms,
            end_ms,
            ?sample_rate,
            "loading historic metric range"
        );
        let rows = backend.fetch_historic_range(T::table_name(), start_ms, end_ms, sample_rate)?;
        self.load_rows::<T>(store, rows)
    }

    fn load_rows<T: PersistentMetric>(
        &mut self,
        store: &mut BufferStore,
        rows: Vec<String>,
    ) -> Result<(), String> {
        for row in rows {
            let fields: Vec<_> = row.split(',').collect();
            if fields.len() < 2 {
                continue;
            }
            let timestamp_ms = fields[0]
                .parse::<i64>()
                .map_err(|error| error.to_string())?;
            store.push_sample(MetricSample {
                timestamp_ms,
                data: T::from_sql_row(&fields[1..])?,
            });
        }
        Ok(())
    }

    // Flushes new persistent samples to the backend when the configured
    // interval has elapsed.
    // pub fn flush(
    //     &mut self,
    //     store: &mut BufferStore,
    //     timestamp_ms: i64,
    //     backend: &mut dyn StorageBackend,
    // ) -> Result<(), String> {
    //     if timestamp_ms - self.last_flush_timestamp_ms < self.flush_interval_ms {
    //         return Ok(());
    //     }

    //     info!(
    //         timestamp_ms,
    //         previous_flush_timestamp_ms = self.last_flush_timestamp_ms,
    //         codec_count = self.codecs.len(),
    //         "flushing metric buffers to persistent storage"
    //     );
    //     let mut flushed_buffer_count = 0;
    //     for (metric_id, codec) in &self.codecs {
    //         let watermark = *self.flush_watermarks.get(metric_id).unwrap_or(&0);
    //         let samples = store.samples_since(*metric_id, watermark);
    //         if samples.is_empty() {
    //             continue;
    //         }
    //         flushed_buffer_count += 1;

    //         let rows: Vec<Vec<String>> = samples
    //             .iter()
    //             .filter_map(|(timestamp, data)| (codec.encode_row)(*timestamp, data.as_ref()))
    //             .collect();
    //         let high_watermark = samples
    //             .iter()
    //             .map(|(timestamp, _)| *timestamp)
    //             .max()
    //             .unwrap_or(watermark);

    //         backend.flush_batch(codec.table, codec.columns, &rows)?;
    //         self.flush_watermarks.insert(*metric_id, high_watermark);
    //     }

    //     self.last_flush_timestamp_ms = timestamp_ms;
    //     info!(flushed_buffer_count, "metric storage flush completed");

    //     Ok(())
    // }
}
