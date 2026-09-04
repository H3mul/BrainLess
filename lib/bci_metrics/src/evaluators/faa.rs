use crate::metrics::{FaaMetric, RawEegMetric};
use metric_engine::{
    MetricDependency, MetricEvaluator, TickLedger, engine::buffer_store::ReadOnlyBufferStore,
};
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
            .get_buffer::<RawEegMetric>()?
            .get_sample_for_ts(timestamp_ts)
            .ok_or("No raw EEG available")?;
        Ok(FaaMetric {
            faa: (eeg.data.af8 - eeg.data.af7).abs(),
        })
    }
}
