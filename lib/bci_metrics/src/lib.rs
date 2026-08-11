pub mod evaluators;
pub mod metrics;

pub use evaluators::*;
pub use metric_engine::DuckDbBackend;
use metric_engine::{MetricEngine, MetricRegistration, boxed_evaluator};
pub use metrics::*;
use std::collections::HashSet;

pub fn build_engine() -> Result<MetricEngine, String> {
    MetricEngine::builder()
        .with_metrics(HashSet::from([
            MetricRegistration::ephemeral::<RawEegMetric>(),
            MetricRegistration::ephemeral::<FaaMetric>(),
            MetricRegistration::ephemeral::<FaaVolatilityMetric>(),
            MetricRegistration::ephemeral::<TempRatioMetric>(),
        ]))
        .with_evaluators(HashSet::from([
            boxed_evaluator(FaaEvaluator),
            boxed_evaluator(FaaVolatilityEvaluator),
            boxed_evaluator(TempRatioEvaluator),
        ]))
        .build()
}
