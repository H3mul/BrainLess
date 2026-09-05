use crate::{
    BufferStore, MetricDependency, MetricEvaluator, MetricGroup, MetricId,
    engine::buffer_store::ReadOnlyBufferStore,
};
use std::collections::HashSet;
use std::sync::Arc;

pub mod dependency_graph;
pub mod tick_executor;

pub trait StagedOutput: Send {
    fn commit_to_store(self: Box<Self>, timestamp_ms: i64, store: &mut BufferStore);
}

struct ConcreteStagedOutput<G: MetricGroup> {
    group: G,
}

impl<G: MetricGroup> StagedOutput for ConcreteStagedOutput<G> {
    fn commit_to_store(self: Box<Self>, timestamp_ms: i64, store: &mut BufferStore) {
        self.group.commit_to_store(timestamp_ms, store);
    }
}

/// Object-safe evaluator interface used by the compiled execution plan.
pub trait ErasedEvaluator: Send + Sync {
    /// Stable evaluator identifier.
    fn id(&self) -> &'static str;
    /// Concrete metric types produced by the evaluator.
    fn produces(&self) -> HashSet<MetricId>;
    /// Dependencies required by the evaluator.
    fn dependencies(&self) -> HashSet<MetricDependency>;
    /// Evaluates and commits the erased output to storage.
    fn evaluate_erased(
        &self,
        timestamp_ms: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Box<dyn StagedOutput>, String>;
}

impl<E> ErasedEvaluator for E
where
    E: MetricEvaluator + 'static,
    E::Output: MetricGroup,
{
    fn id(&self) -> &'static str {
        <E as MetricEvaluator>::id()
    }
    fn produces(&self) -> HashSet<MetricId> {
        E::Output::type_ids()
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        <E as MetricEvaluator>::dependencies(self)
    }
    fn evaluate_erased(
        &self,
        timestamp_ms: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Box<dyn StagedOutput>, String> {
        let output = self.evaluate(timestamp_ms, store)?;
        Ok(Box::new(ConcreteStagedOutput { group: output }))
    }
}

/// A collection of evaluators with no dependencies between one another.
pub struct ExecutionStage {
    pub evaluators: Vec<Arc<dyn ErasedEvaluator>>,
}

/// Dependency-ordered evaluator stages for one engine tick.
pub struct ExecutionPlan {
    pub stages: Vec<ExecutionStage>,
}
