pub mod db;
pub mod engine;
pub mod execution;

#[cfg(test)]
pub(crate) mod test_fixtures;

pub use db::backend::{NoopStorageBackend, StorageBackend};
pub use db::persistence::{PersistenceDriver, PersistentMetric};
#[cfg(feature = "timescaledb")]
pub use db::timescaledb_impl::TimescaleDbStorageBackend;
pub use engine::buffer_store::{BufferStore, TickResult, TickSample, TimeSeriesBuffer};
pub use engine::core::{
    Age, Metric, MetricDependency, MetricEvaluator, MetricGroup, MetricId, MetricSample,
    SampleRate, SampleRequest,
};
pub use engine::sessions::{LiveSession, LiveSessionConfig};
pub use engine::{
    EvaluatorRegistration, MetricEngine, MetricEngineBuilder, MetricRegistration, boxed_evaluator,
};
