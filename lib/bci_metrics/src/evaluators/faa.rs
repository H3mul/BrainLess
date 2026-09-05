use crate::metrics::{FaaMetric, RawEegMetric};
use metric_engine::{MetricDependency, MetricEvaluator, engine::buffer_store::ReadOnlyBufferStore};
use std::collections::HashSet;

/// Computes frontal alpha asymmetry from the latest raw EEG sample.
pub struct FaaEvaluator;

impl MetricEvaluator for FaaEvaluator {
    type Output = FaaMetric;
    fn id() -> &'static str {
        "faa_evaluator"
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([MetricDependency::latest::<RawEegMetric>()])
    }
    fn evaluate(
        &self,
        timestamp_ts: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Self::Output, String> {
        let eeg = store
            .get_buffer::<RawEegMetric>()
            .ok_or_else(|| "Failed to fetch Eeg buffer".to_string())?
            .get_sample_for_ts(timestamp_ts)
            .ok_or_else(|| {
                "No raw EEG available for timestamp: ".to_string() + &timestamp_ts.to_string()
            })?;
        Ok(FaaMetric {
            faa: (eeg.data.af8 - eeg.data.af7).abs(),
        })
    }
}
