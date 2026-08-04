use crate::metrics::{FaaMetric, TempRatioMetric};
use metric_engine::{MetricDependency, MetricEvaluator, TickLedger};
use std::collections::HashSet;

/// Computes the experimental temporary ratio from the latest FAA sample.
pub struct TempRatioEvaluator;

impl MetricEvaluator for TempRatioEvaluator {
    type Output = TempRatioMetric;
    fn id(&self) -> &'static str {
        "temp_ratio_evaluator"
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([MetricDependency::latest::<FaaMetric>()])
    }
    fn evaluate(&self, ledger: &TickLedger) -> Result<Self::Output, String> {
        let faa = ledger
            .get::<FaaMetric>()?
            .latest()
            .ok_or("No FAA available")?;
        Ok(TempRatioMetric {
            ratio: faa.data.faa * 1.5,
        })
    }
}
