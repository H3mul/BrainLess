use crate::core::{Metric, MetricEvaluator, MetricGroup, MetricSample, ReadOnlyTickLedger};
use crate::dag::{CompiledSessionResources, DagCompiler, ErasedEvaluator, ExecutionMode};
use crate::storage::{NoopStorageBackend, PersistentMetric, StorageBackend, StorageEngine};
use std::any::TypeId;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use tracing::{debug, info, warn};

type BufferRegistration = Box<dyn FnOnce(&mut StorageEngine, i64) + Send>;

/// Hashable registration wrapper for a typed evaluator.
pub struct EvaluatorRegistration {
    pub id: &'static str,
    evaluator: Box<dyn ErasedEvaluator>,
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

/// Wraps a typed evaluator for insertion into a `HashSet`.
pub fn boxed_evaluator<E>(evaluator: E) -> EvaluatorRegistration
where
    E: MetricEvaluator + 'static,
    E::Output: MetricGroup,
{
    let id = evaluator.id();
    EvaluatorRegistration {
        id,
        evaluator: Box::new(evaluator),
    }
}

/// Runtime metric registration describing persistence intent.
pub struct MetricRegistration {
    pub metric_type: TypeId,
    pub persistence: bool,
    register: Option<BufferRegistration>,
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
    /// Creates an ephemeral registration for any metric type.
    pub fn ephemeral<T: Metric>() -> Self {
        Self {
            metric_type: TypeId::of::<T>(),
            persistence: false,
            register: Some(Box::new(move |storage, buffer_size_ms| {
                storage.register_ephemeral_buffer::<T>(buffer_size_ms)
            })),
        }
    }

    /// Creates a persistent registration for a metric with a persistence mapping.
    pub fn persistent<T: PersistentMetric>() -> Self {
        Self {
            metric_type: TypeId::of::<T>(),
            persistence: true,
            register: Some(Box::new(move |storage, buffer_size_ms| {
                storage.register_buffer::<T>(buffer_size_ms)
            })),
        }
    }

    /// Creates a registration from a metric type and persistence flag.
    pub fn new<T: Metric>(persistence: bool) -> Self {
        if persistence {
            Self {
                metric_type: TypeId::of::<T>(),
                persistence: true,
                register: None,
            }
        } else {
            Self::ephemeral::<T>()
        }
    }
}

/// Configures evaluators, buffers, targets, and persistence before compilation.
pub struct MetricEngineBuilder {
    evaluators: Vec<Box<dyn ErasedEvaluator>>,
    target_metrics: Option<HashSet<TypeId>>,
    source_metrics: HashSet<TypeId>,
    storage_backend: Option<Box<dyn StorageBackend>>,
    flush_interval_ms: Option<i64>,
    buffer_size_ms: Option<i64>,
    registrations: Vec<MetricRegistration>,
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
            target_metrics: None,
            source_metrics: HashSet::new(),
            storage_backend: None,
            flush_interval_ms: None,
            buffer_size_ms: None,
            registrations: Vec::new(),
            validation_error: None,
        }
    }

    /// Registers and validates a batch of application evaluators for DAG compilation.
    pub fn with_evaluators(mut self, evaluators: HashSet<EvaluatorRegistration>) -> Self {
        for registration in evaluators {
            let evaluator = registration.evaluator;
            let evaluator_id = evaluator.id();
            let output_types = evaluator.produces();
            let dependency_types: HashSet<TypeId> = evaluator
                .dependencies()
                .into_iter()
                .map(|dependency| dependency.type_id)
                .collect();
            let conflicts: HashSet<TypeId> = output_types
                .intersection(&dependency_types)
                .copied()
                .collect();
            if !conflicts.is_empty() {
                let error = format!(
                    "Evaluator '{}' produces metrics that it also depends on: {:?}; evaluator outputs and dependencies must be disjoint",
                    evaluator_id, conflicts
                );
                warn!(
                    evaluator = evaluator_id,
                    ?conflicts,
                    "invalid evaluator registration"
                );
                self.validation_error.get_or_insert(error);
            }
            debug!(evaluator = evaluator_id, "registering metric evaluator");
            self.evaluators.push(evaluator);
        }
        self
    }

    /// Registers metrics with runtime-selected persistence policies.
    pub fn with_metrics(mut self, metrics: HashSet<MetricRegistration>) -> Self {
        if let Some(registration) = metrics
            .iter()
            .find(|registration| registration.persistence && registration.register.is_none())
        {
            let error = format!(
                "Metric type {:?} requested persistence but has no PersistentMetric mapping",
                registration.metric_type
            );
            warn!(%error, metric_type = ?registration.metric_type, "invalid metric registration");
            self.validation_error.get_or_insert(error);
        }
        self.registrations.extend(metrics);
        self
    }

    /// Sets the timestamp retention window for every metric buffer in milliseconds.
    pub fn with_buffer_size(mut self, buffer_size_ms: i64) -> Self {
        self.buffer_size_ms = Some(buffer_size_ms.max(1));
        self
    }

    pub fn with_storage(mut self, backend: impl StorageBackend + 'static) -> Self {
        self.storage_backend = Some(Box::new(backend));
        self
    }

    /// Optionally restricts the required metric set to a subset of registered metrics.
    pub fn with_output_metrics(mut self, targets: HashSet<TypeId>) -> Self {
        self.target_metrics = Some(targets);
        self
    }

    /// Declares metric types supplied externally at runtime.
    pub fn with_source_metrics(mut self, sources: HashSet<TypeId>) -> Self {
        self.source_metrics = sources;
        self
    }

    /// Sets the storage flush interval in milliseconds.
    pub fn with_storage_flush_interval(mut self, interval_ms: i64) -> Self {
        self.flush_interval_ms = Some(interval_ms.max(1));
        self
    }

    pub fn build(self) -> Result<MetricEngine, String> {
        if let Some(error) = self.validation_error.as_ref() {
            return Err(error.clone());
        }
        if self.storage_backend.is_none()
            && self
                .registrations
                .iter()
                .any(|registration| registration.persistence)
        {
            let error = "Persistent metrics were registered, but no storage backend was configured; configure storage or register only ephemeral metrics";
            warn!(%error, "invalid persistence configuration");
            return Err(error.into());
        }

        let buffer_size_ms = self.buffer_size_ms.unwrap_or(300_000);
        let flush_interval_ms = self.flush_interval_ms.unwrap_or_else(|| {
            if self.buffer_size_ms.is_some() {
                (buffer_size_ms / 2).max(1)
            } else {
                20_000
            }
        });
        let targets = self.target_metrics.clone().unwrap_or_else(|| {
            self.registrations
                .iter()
                .map(|registration| registration.metric_type)
                .collect()
        });

        info!(
            evaluator_count = self.evaluators.len(),
            target_count = targets.len(),
            source_count = self.source_metrics.len(),
            buffer_registration_count = self.registrations.len(),
            buffer_size_ms,
            flush_interval_ms,
            "building metric engine"
        );
        let resources = DagCompiler::compile(
            &targets,
            &self.source_metrics,
            self.evaluators,
            ExecutionMode::Sequential,
        )?;
        let backend = self
            .storage_backend
            .unwrap_or_else(|| Box::new(NoopStorageBackend));
        let mut storage = StorageEngine::new(flush_interval_ms, backend);
        for registration in self.registrations {
            if let Some(register) = registration.register {
                register(&mut storage, buffer_size_ms);
            }
        }
        Ok(MetricEngine { storage, resources })
    }
}

/// Orchestrates ingestion, ledger provisioning, DAG evaluation, and storage flushes.
pub struct MetricEngine {
    storage: StorageEngine,
    resources: CompiledSessionResources,
}

impl MetricEngine {
    pub fn builder() -> MetricEngineBuilder {
        MetricEngineBuilder::new()
    }

    pub fn ingest_sample<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.storage
            .commit_sample(MetricSample { timestamp_ms, data });
    }

    pub fn feed<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.ingest_sample(timestamp_ms, data)
    }

    pub fn ingest_external_sample<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.ingest_sample(timestamp_ms, data)
    }

    pub fn feed_external<T: Metric>(&mut self, timestamp_ms: i64, data: T) {
        self.feed(timestamp_ms, data)
    }

    /// Executes one tick and returns a read-only view of the complete ledger.
    pub fn tick(&mut self, timestamp_ms: i64) -> Result<ReadOnlyTickLedger, String> {
        let mut ledger = self
            .storage
            .provision_ledger(timestamp_ms, &self.resources.aggregate_demand);
        self.resources
            .execution_plan
            .execute(&mut ledger, &mut self.storage, timestamp_ms)?;
        self.storage.maybe_flush(timestamp_ms)?;
        Ok(ReadOnlyTickLedger::new(ledger))
    }
}
