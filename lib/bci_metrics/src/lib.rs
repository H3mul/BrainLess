pub mod evaluators;
pub mod metrics;

pub use evaluators::*;
pub use metric_engine::DuckDbBackend;
use metric_engine::{MetricEngine, MetricRegistration, boxed_evaluator};
pub use metrics::*;
use std::any::TypeId;
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
        .with_buffer_size(30_000)
        .with_source_metrics(HashSet::from([TypeId::of::<RawEegMetric>()]))
        .with_output_metrics(HashSet::from([TypeId::of::<FaaVolatilityMetric>()]))
        .build()
}
