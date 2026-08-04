use crate::metrics::{FaaMetric, RawEegMetric};
use metric_engine::{MetricDependency, MetricEvaluator, TickLedger};
use std::collections::HashSet;

pub struct FaaEvaluator;

impl MetricEvaluator for FaaEvaluator {
    type Output = FaaMetric;
    fn id(&self) -> &'static str {
        "faa_evaluator"
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([MetricDependency::latest::<RawEegMetric>()])
    }
    fn evaluate(&self, ledger: &TickLedger) -> Result<Self::Output, String> {
        let eeg = ledger
            .get::<RawEegMetric>()?
            .latest()
            .ok_or("No raw EEG available")?;
        Ok(FaaMetric {
            faa: (eeg.data.af8 - eeg.data.af7).abs(),
        })
    }
}
