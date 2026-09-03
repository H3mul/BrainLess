use crate::NoopStorageBackend;
use crate::core::{Metric, MetricDependency, MetricEvaluator, MetricGroup};
use crate::dag::ErasedEvaluator;
pub mod sessions;
use self::sessions::{LiveSession, LiveSessionConfig};
use crate::storage::{BufferStore, PersistentMetric, StorageBackend};
use std::any::TypeId;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{info, warn};

/// Erased buffer registration for a metric type. A plain function pointer: the
/// constructors never capture state, so every registration is guaranteed to have
/// one and no metric can silently lack a buffer.
type BufferRegistration =
    fn(&mut BufferStore, i64, &HashSet<MetricDependency>) -> Result<(), String>;

pub struct EvaluatorRegistration {
    pub id: &'static str,
    pub(crate) evaluator: Arc<dyn ErasedEvaluator>,
}
impl PartialEq for EvaluatorRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for EvaluatorRegistration {}
impl Hash for EvaluatorRegistration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
pub fn boxed_evaluator<E>(evaluator: E) -> EvaluatorRegistration
where
    E: MetricEvaluator + 'static,
    E::Output: MetricGroup,
{
    let id = evaluator.id();
    EvaluatorRegistration {
        id,
        evaluator: Arc::new(evaluator),
    }
}

#[derive(Clone, Copy)]
pub struct MetricRegistration {
    pub(crate) metric_type: TypeId,
    pub(crate) persistence: bool,
    pub(crate) register: BufferRegistration,
}

impl PartialEq for MetricRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.metric_type == other.metric_type && self.persistence == other.persistence
    }
}

impl Eq for MetricRegistration {}
impl Hash for MetricRegistration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric_type.hash(state);
        self.persistence.hash(state);
    }
}

impl MetricRegistration {
    pub fn ephemeral<T: Metric>() -> Self {
        Self {
            metric_type: TypeId::of::<T>(),
            persistence: false,
            register: |storage, buffer_size_ms, _demand| {
                storage.register_ephemeral_buffer::<T>(buffer_size_ms);
                Ok(())
            },
        }
    }
    pub fn persistent<T: PersistentMetric>() -> Self {
        Self {
            metric_type: TypeId::of::<T>(),
            persistence: true,
            register: BufferStore::register_buffer::<T>,
        }
    }
}

/// Immutable declarations and the compiled evaluator graph shared by sessions.
pub struct MetricEngineBuilder {
    evaluators: Vec<Arc<dyn ErasedEvaluator>>,
    metrics: Vec<MetricRegistration>,
    validation_error: Option<String>,
}

impl Default for MetricEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricEngineBuilder {
    pub fn new() -> Self {
        Self {
            evaluators: Vec::new(),
            metrics: Vec::new(),
            validation_error: None,
        }
    }
    pub fn register_evaluator<E: MetricEvaluator + 'static>(self, evaluator: E) -> Self
    where
        E::Output: MetricGroup,
    {
        self.with_evaluators(HashSet::from([boxed_evaluator(evaluator)]))
    }
    pub fn with_evaluators(mut self, evaluators: HashSet<EvaluatorRegistration>) -> Self {
        for registration in evaluators {
            let evaluator = registration.evaluator;
            let conflicts: HashSet<_> = evaluator
                .produces()
                .intersection(
                    &evaluator
                        .dependencies()
                        .into_iter()
                        .map(|d| d.type_id)
                        .collect(),
                )
                .copied()
                .collect();
            if !conflicts.is_empty() {
                let error = format!(
                    "Evaluator '{}' outputs and dependencies intersect: {:?}",
                    evaluator.id(),
                    conflicts
                );
                warn!(%error);
                self.validation_error.get_or_insert(error);
            }
            self.evaluators.push(evaluator);
        }
        self
    }

    pub fn with_metrics(mut self, metrics: HashSet<MetricRegistration>) -> Self {
        self.metrics.extend(metrics);
        self
    }

    pub fn build(self) -> Result<MetricEngine, String> {
        if let Some(error) = self.validation_error {
            return Err(error);
        }

        info!(
            evaluator_count = self.evaluators.len(),
            metric_count = self.metrics.len(),
            "building immutable metric engine definition"
        );

        Ok(MetricEngine {
            evaluators: self.evaluators,
            metrics: self.metrics,
        })
    }
}

/// Collection of Evaluator and Metric implementations for the metric engine.
pub struct MetricEngine {
    pub evaluators: Vec<Arc<dyn ErasedEvaluator>>,
    pub metrics: Vec<MetricRegistration>,
}

impl MetricEngine {
    pub fn builder() -> MetricEngineBuilder {
        MetricEngineBuilder::new()
    }
    pub fn live_session(
        &self,
        config: LiveSessionConfig,
        backend: impl StorageBackend + 'static,
    ) -> Result<LiveSession, String> {
        if backend.is_noop() && self.metrics.iter().any(|metric| metric.persistence) {
            return Err(
                "ephemeral live sessions cannot use persistent metric registrations".into(),
            );
        }
        Ok(LiveSession::new(config, Box::new(backend), self))
    }

    pub fn ephemeral_live_session(&self, config: LiveSessionConfig) -> Result<LiveSession, String> {
        self.live_session(config, NoopStorageBackend)
    }
    // pub fn replay_session(
    //     &self,
    //     config: ReplaySessionConfig,
    //     backend: impl StorageBackend + 'static,
    // ) -> Result<ReplaySession, String> {
    //     let runtime_config = RuntimeConfig::default();
    //     Ok(ReplaySession::new(config, runtime))
    // }
}
