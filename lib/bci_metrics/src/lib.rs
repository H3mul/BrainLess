pub mod evaluators;
pub mod metrics;

pub use evaluators::*;
pub use metric_engine::DuckDbBackend;
use metric_engine::MetricEngine;
pub use metrics::*;
use std::any::TypeId;

pub fn build_engine() -> Result<MetricEngine, String> {
    MetricEngine::builder()
        .register_persistent_metric::<RawEegMetric>(30)
        .register_persistent_metric::<FaaMetric>(300)
        .register_persistent_metric::<FaaVolatilityMetric>(300)
        .register_ephemeral_metric::<TempRatioMetric>(300)
        .register_evaluator(FaaEvaluator)
        .register_evaluator(FaaVolatilityEvaluator)
        .register_evaluator(TempRatioEvaluator)
        .with_storage(DuckDbBackend::new("./sessions.duckdb"))
        .with_targets(vec![TypeId::of::<FaaVolatilityMetric>()])
        .build()
}
