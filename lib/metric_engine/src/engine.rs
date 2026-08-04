use crate::core::{ExternalMetric, Metric, MetricEvaluator, MetricGroup, MetricSample};
use crate::dag::{CompiledSessionResources, DagCompiler, ExecutionMode};
use crate::storage::{NoopStorageBackend, StorageBackend, StorageEngine};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};

pub struct TickOutputs {
    pub timestamp_ms: i64,
    outputs: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl TickOutputs {
    pub fn new(timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms,
            outputs: HashMap::new(),
        }
    }

    pub fn insert_erased(&mut self, id: TypeId, value: Box<dyn Any + Send + Sync>) {
        self.outputs.insert(id, value);
    }

    pub fn get<T: Metric>(&self) -> Option<&MetricSample<T>> {
        self.outputs
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref())
    }

    pub fn get_data<T: Metric>(&self) -> Option<&T> {
        self.get::<T>().map(|s| &s.data)
    }

    pub fn get_latest<T: Metric>(&self) -> Option<&T> {
        self.get_data::<T>()
    }
}

type BufferRegistration = Box<dyn FnOnce(&mut StorageEngine) + Send>;

pub struct MetricEngineBuilder {
    evaluators: Vec<Box<dyn crate::core::ErasedEvaluator>>,
    target_metrics: Vec<TypeId>,
    storage_backend: Option<Box<dyn StorageBackend>>,
    flush_interval_ms: i64,
    registrations: Vec<BufferRegistration>,
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
            target_metrics: Vec::new(),
            storage_backend: None,
            flush_interval_ms: 20_000,
            registrations: Vec::new(),
        }
    }

    pub fn register_evaluator<E: MetricEvaluator + 'static>(mut self, evaluator: E) -> Self
    where
        E::Output: MetricGroup,
    {
        self.evaluators.push(Box::new(evaluator));

        self
    }

    pub fn register_persistent_metric<T: crate::core::TsdbStorage>(
        mut self,
        capacity: usize,
    ) -> Self {
        self.registrations
            .push(Box::new(move |s| s.register_buffer::<T>(capacity)));

        self
    }

    pub fn register_ephemeral_metric<T: Metric>(mut self, capacity: usize) -> Self {
        self.registrations.push(Box::new(move |s| {
            s.register_ephemeral_buffer::<T>(capacity)
        }));

        self
    }

    pub fn with_storage(mut self, backend: impl StorageBackend + 'static) -> Self {
        self.storage_backend = Some(Box::new(backend));

        self
    }

    pub fn with_targets(mut self, targets: Vec<TypeId>) -> Self {
        self.target_metrics = targets;

        self
    }

    pub fn with_flush_interval_ms(mut self, interval: i64) -> Self {
        self.flush_interval_ms = interval;

        self
    }

    pub fn build(self) -> Result<MetricEngine, String> {
        let targets = self.target_metrics.iter().copied().collect();

        let resources = DagCompiler::compile(
            &self.target_metrics,
            self.evaluators,
            ExecutionMode::Sequential,
        )?;

        let backend = self
            .storage_backend
            .unwrap_or_else(|| Box::new(NoopStorageBackend));

        let mut storage = StorageEngine::new(self.flush_interval_ms, backend);

        for registration in self.registrations {
            registration(&mut storage);
        }

        Ok(MetricEngine {
            storage,
            resources,
            target_metric_types: targets,
        })
    }
}

pub struct MetricEngine {
    storage: StorageEngine,
    resources: CompiledSessionResources,
    target_metric_types: HashSet<TypeId>,
}

impl MetricEngine {
    pub fn builder() -> MetricEngineBuilder {
        MetricEngineBuilder::new()
    }

    pub fn ingest_external_sample<T: ExternalMetric>(&mut self, timestamp_ms: i64, data: T) {
        self.storage
            .commit_sample(MetricSample { timestamp_ms, data });
    }

    pub fn feed_external<T: ExternalMetric>(&mut self, timestamp_ms: i64, data: T) {
        self.ingest_external_sample(timestamp_ms, data)
    }

    pub fn tick(&mut self, timestamp_ms: i64) -> Result<TickOutputs, String> {
        let mut ledger = self
            .storage
            .provision_ledger(timestamp_ms, &self.resources.aggregate_demand);

        self.resources
            .execution_plan
            .execute(&mut ledger, &mut self.storage, timestamp_ms)?;

        self.storage.maybe_flush(timestamp_ms)?;

        let mut output = TickOutputs::new(timestamp_ms);

        for id in &self.target_metric_types {
            if let Some(sample) = self.storage.latest_erased(*id) {
                output.insert_erased(*id, sample)
            }
        }

        Ok(output)
    }
}
