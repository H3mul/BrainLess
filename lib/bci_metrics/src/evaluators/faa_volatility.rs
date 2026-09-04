use crate::metrics::{FaaMetric, FaaVolatilityMetric};
use metric_engine::{MetricDependency, MetricEvaluator, SampleRate, TickLedger};
use std::collections::HashSet;

/// Computes the aggregate volatility over the configured FAA history window.
pub struct FaaVolatilityEvaluator;

impl MetricEvaluator for FaaVolatilityEvaluator {
    type Output = FaaVolatilityMetric;
    fn id() -> &'static str {
        "faa_volatility_evaluator"
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        HashSet::from([
            MetricDependency::latest::<FaaMetric>(),
            MetricDependency::window::<FaaMetric>(300, 0, SampleRate::Best),
        ])
    }
    fn evaluate(&self, ledger: &TickLedger) -> Result<Self::Output, String> {
        let faa = ledger.get::<FaaMetric>()?;
        let sum: f32 = faa.as_slice().iter().map(|sample| sample.data.faa).sum();
        Ok(FaaVolatilityMetric {
            volatility: sum / faa.len().max(1) as f32,
        })
    }
}
