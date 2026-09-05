//! Shared in-crate fixtures for engine and persistence tests.

use crate::db::backend::StorageBackend;
use crate::db::persistence::PersistentMetric;
use crate::engine::buffer_store::ReadOnlyBufferStore;
use crate::engine::core::{Metric, MetricDependency, MetricEvaluator, SampleRate};

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Externally fed source metric.
#[derive(Debug, Clone)]
pub struct TestSource {
    pub value: f64,
}
impl Metric for TestSource {
    fn id() -> &'static str {
        "test_source"
    }
}

/// Derived metric persisted through the recording backend.
#[derive(Debug, Clone)]
pub struct TestDerived {
    pub value: f64,
}
impl Metric for TestDerived {
    fn id() -> &'static str {
        "test_derived"
    }
}
impl PersistentMetric for TestDerived {
    fn table_name() -> &'static str {
        "test_derived"
    }
    fn schema_columns() -> &'static [&'static str] {
        &["value"]
    }
    fn create_table_sql() -> &'static str {
        "CREATE TABLE test_derived (timestamp_ms BIGINT, value DOUBLE)"
    }
    fn insert_sql() -> &'static str {
        "INSERT INTO test_derived VALUES (?, ?)"
    }
    fn select_range_sql() -> &'static str {
        "SELECT timestamp_ms, value FROM test_derived"
    }
    fn to_sql_params(&self) -> Vec<String> {
        vec![self.value.to_string()]
    }
    fn from_sql_row(row_params: &[&str]) -> Result<Self, String> {
        let value = row_params
            .first()
            .ok_or_else(|| "missing value column".to_string())?
            .parse::<f64>()
            .map_err(|error| error.to_string())?;
        Ok(Self { value })
    }
}

/// Produces `TestDerived` by doubling the latest `TestSource` sample.
pub struct DoublingEvaluator;
impl MetricEvaluator for DoublingEvaluator {
    type Output = TestDerived;

    fn id() -> &'static str {
        "doubling_evaluator"
    }

    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([MetricDependency::latest::<TestSource>()])
    }

    fn evaluate(
        &self,
        timestamp_ms: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Self::Output, String> {
        let source = store
            .get_buffer::<TestSource>()
            .and_then(|buffer| buffer.get_sample_last_before_ts(timestamp_ms))
            .ok_or_else(|| "no source sample available".to_string())?;
        Ok(TestDerived {
            value: source.data.value * 2.0,
        })
    }
}

/// Ephemeral aggregate produced from a 60-second source window; its window
/// dependency is what should drive the source buffer's retention floor in
/// tests.
#[derive(Debug, Clone)]
pub struct TestAggMetric {
    pub count: f64,
}
impl Metric for TestAggMetric {
    fn id() -> &'static str {
        "test_agg_metric"
    }
}

/// Produces `TestAggMetric` holding the number of source samples in the
/// last 60 seconds.
pub struct WindowCountEvaluator;
impl MetricEvaluator for WindowCountEvaluator {
    type Output = TestAggMetric;

    fn id() -> &'static str {
        "window_count_evaluator"
    }

    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([MetricDependency::window::<TestSource>(
            60,
            0,
            SampleRate::Best,
        )])
    }

    fn evaluate(
        &self,
        timestamp_ms: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Self::Output, String> {
        let count = store
            .get_buffer::<TestSource>()
            .map(|buffer| {
                buffer
                    .get_samples_in_ts_window(timestamp_ms - 60_000, timestamp_ms)
                    .len()
            })
            .unwrap_or(0);
        Ok(TestAggMetric {
            count: count as f64,
        })
    }
}

#[derive(Debug, Default)]
pub struct RecordingState {
    pub flushes: Vec<RecordedFlush>,
    pub failing: bool,
}

#[derive(Debug, Clone)]
pub struct RecordedFlush {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Storage backend that records flush batches in shared state so tests can
/// keep a handle while the driver owns the backend itself.
#[derive(Clone, Debug, Default)]
pub struct RecordingBackend {
    pub state: Arc<Mutex<RecordingState>>,
}

impl StorageBackend for RecordingBackend {
    fn is_noop(&self) -> bool {
        false
    }

    fn flush_batch(
        &mut self,
        table_name: &str,
        schema_columns: &[&str],
        row_values: &[Vec<String>],
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if state.failing {
            return Err("simulated backend failure".to_string());
        }
        state.flushes.push(RecordedFlush {
            table: table_name.to_string(),
            columns: schema_columns
                .iter()
                .map(|column| column.to_string())
                .collect(),
            rows: row_values.to_vec(),
        });
        Ok(())
    }

    fn fetch_historic_range(
        &self,
        _: &str,
        _: i64,
        _: i64,
        _: SampleRate,
    ) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    fn fetch_historic(&self, _: &str, _: i64, _: SampleRate) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}
