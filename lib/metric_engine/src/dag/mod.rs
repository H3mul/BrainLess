use crate::core::{MetricDependency, TickLedger};
use crate::engine::buffer_store::BufferStore;
use crate::{MetricEvaluator, MetricGroup};
use std::any::TypeId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::debug;

/// Object-safe evaluator interface used by the compiled execution plan.
pub trait ErasedEvaluator: Send + Sync {
    /// Stable evaluator identifier.
    fn id(&self) -> &'static str;
    /// Concrete metric types produced by the evaluator.
    fn produces(&self) -> HashSet<TypeId>;
    /// Dependencies required by the evaluator.
    fn dependencies(&self) -> HashSet<MetricDependency>;
    /// Evaluates and commits the erased output to storage.
    fn evaluate_and_commit(
        &self,
        ledger: &TickLedger,
        storage: &mut BufferStore,
        timestamp_ms: i64,
    ) -> Result<(), String>;
}

impl<E> ErasedEvaluator for E
where
    E: MetricEvaluator + 'static,
    E::Output: MetricGroup,
{
    fn id(&self) -> &'static str {
        <E as MetricEvaluator>::id(self)
    }
    fn produces(&self) -> HashSet<TypeId> {
        E::Output::type_ids()
    }
    fn dependencies(&self) -> HashSet<MetricDependency> {
        <E as MetricEvaluator>::dependencies(self)
    }
    fn evaluate_and_commit(
        &self,
        ledger: &TickLedger,
        storage: &mut BufferStore,
        timestamp_ms: i64,
    ) -> Result<(), String> {
        self.evaluate(ledger)?
            .commit_to_storage(storage, timestamp_ms);
        Ok(())
    }
}

/// Execution strategy for a compiled evaluator plan.
pub enum ExecutionMode {
    Sequential,
    Parallel,
}

/// A collection of evaluators with no dependencies between one another.
pub struct ExecutionStage {
    pub evaluators: Vec<Arc<dyn ErasedEvaluator>>,
}

/// Dependency-ordered evaluator stages for one engine tick.
pub struct ExecutionPlan {
    pub stages: Vec<ExecutionStage>,
    pub mode: ExecutionMode,
}

impl ExecutionPlan {
    /// Runs stages in dependency order. Evaluators within a stage are independent.
    pub fn execute(&self, storage: &mut BufferStore, timestamp_ms: i64) -> Result<(), String> {
        debug!(
            stage_count = self.stages.len(),
            timestamp_ms, "executing metric evaluation plan"
        );
        for (stage_index, stage) in self.stages.iter().enumerate() {
            debug!(
                stage = stage_index,
                evaluator_count = stage.evaluators.len(),
                timestamp_ms,
                "executing metric stage"
            );
            for evaluator in &stage.evaluators {
                debug!(
                    evaluator = evaluator.id(),
                    stage = stage_index,
                    timestamp_ms,
                    "evaluating metric"
                );
                let dependency_ledger =
                    storage.provision_ledger(timestamp_ms, &evaluator.dependencies());
                evaluator.evaluate_and_commit(&dependency_ledger, storage, timestamp_ms)?
            }
        }
        Ok(())
    }
}

pub struct DagGraphTraversal {
    // Stages of parallel execution of Evaluators.
    pub execution_plan: ExecutionPlan,

    // Aggregate set of all dependencies declarations from required evaluators for this traversal.
    pub aggregate_demand: HashSet<MetricDependency>,
}

/// Immutable evaluator dependency graph.
///
/// The graph can be queried repeatedly with different source and target
/// availability sets without rebuilding evaluator relationships.
pub struct DagGraph {
    // Full set of available evaluators (and evaluator index ground truth)
    evaluators: Vec<Arc<dyn ErasedEvaluator>>,

    // map of metric type to the index of the evaluator that produces it
    producers: HashMap<TypeId, usize>,
    // Map of evaluator index to list of evaluator indexes that depend on it (Producer -> Consumers)
    edges: Vec<Vec<usize>>,
}

impl DagGraph {
    /// Builds the local producer-consumer graph
    ///
    /// Graph nodes are evaluators. An edge connects a producer to a consumer
    /// when the consumer depends on a metric produced by the producer.
    pub fn new(evaluators: Vec<Arc<dyn ErasedEvaluator>>) -> Result<Self, String> {
        // Map of metric type to the index of the evaluator that produces it
        let mut producers = HashMap::new();
        for (index, evaluator) in evaluators.iter().enumerate() {
            for metric_type in evaluator.produces() {
                if producers.insert(metric_type, index).is_some() {
                    return Err(format!(
                        "Multiple evaluators produce metric type {:?}",
                        metric_type
                    ));
                }
            }
        }

        // Graph edges: Producer -> Consumers
        let mut edges = vec![Vec::new(); evaluators.len()];
        for (consumer, evaluator) in evaluators.iter().enumerate() {
            for dependency in evaluator.dependencies() {
                if let Some(&producer) = producers.get(&dependency.type_id) {
                    if producer != consumer && !edges[producer].contains(&consumer) {
                        edges[producer].push(consumer);
                    }
                }
            }
        }
        Ok(Self {
            evaluators,
            producers,
            edges,
        })
    }

    /// Use the DAG to
    ///
    /// metrics do not create edges because they are supplied outside the DAG.
    /// The same mutable in-degree state is used to form each independent stage
    /// and advance the traversal to the next dependency layer.
    pub fn traverse(
        &self,
        source_metrics: &HashSet<TypeId>,
        target_metrics: &HashSet<TypeId>,
        mode: ExecutionMode,
    ) -> Result<DagGraphTraversal, String> {
        // Set of evaluator indexes that are required to produce the requested target metrics
        let mut required_producers = HashSet::new();

        // Set of metric types that have been visited during the traversal
        let mut visited = HashSet::new();
        // Operational queue of metric types to figure out producers for. Start with the requested target metrics and work backwards to their dependencies.
        let mut work: Vec<TypeId> = target_metrics.iter().copied().collect();
        while let Some(metric_type) = work.pop() {
            // Already accounted for this metric's producers, or it's a source metric
            // Note: this means if a metric is in both in sources and targets = no action.
            if !visited.insert(metric_type) || source_metrics.contains(&metric_type) {
                continue;
            }

            // Fetch metric's producer and add it to the required set
            let evaluator_index = self.producers.get(&metric_type).copied()
                .ok_or_else(|| format!("Metric type {:?} is required but has no evaluator producer and was not declared as a source", metric_type))?;

            if required_producers.insert(evaluator_index) {
                // If this is a newly discovered required producer, add its dependencies to the work queue to be resolved
                work.extend(
                    self.evaluators[evaluator_index]
                        .dependencies()
                        .into_iter()
                        .map(|dependency| dependency.type_id),
                );
            }
        }

        // Collect all dependency declarations of required producers
        let aggregate_demand = required_producers
            .iter()
            .map(|&index| self.evaluators[index].dependencies())
            .flatten()
            .collect::<HashSet<_>>();

        let stages = self.build_stages(&required_producers)?;

        Ok(DagGraphTraversal {
            execution_plan: ExecutionPlan { stages, mode },
            aggregate_demand,
        })
    }

    /// Given a set of required evaluators, divide them into stages of independent
    /// evaluator sets with zero interdependence that can be executed in parallel.
    fn build_stages(
        &self,
        required_producers: &HashSet<usize>,
    ) -> Result<Vec<ExecutionStage>, String> {
        // Degree: counter of edges pointing to a given Evaluator
        //  (the number Evaluators that need to execute before it has all its dependencies satisfied;
        //   a degree of 0 indicates it can be executed immediately)
        let mut degree: HashMap<usize, usize> =
            required_producers.iter().map(|&index| (index, 0)).collect();
        for &producer in required_producers {
            for &consumer in &self.edges[producer] {
                // Only count edges to consumers that are also in the required set:
                // we already flitered it for source metrics
                if required_producers.contains(&consumer) {
                    *degree.get_mut(&consumer).unwrap() += 1;
                }
            }
        }

        // Operational queue of evaluators that are ready to execute (degree 0)
        let mut ready: VecDeque<_> = degree
            .iter()
            .filter_map(|(&index, &value)| (value == 0).then_some(index))
            .collect();

        let mut visited = 0;
        let mut stages = Vec::new();

        while !ready.is_empty() {
            // Flush the ready queue into a stage
            let current: Vec<_> = ready.drain(..).collect();
            visited += current.len();
            stages.push(ExecutionStage {
                evaluators: current
                    .iter()
                    .map(|&index| self.evaluators[index].clone())
                    .collect(),
            });

            // Decrement the current stage's outgoing edges: we have fulfilled these dependencies.
            // Consumers whose in-degree reaches zero become ready for the next stage
            for producer in current {
                for &consumer in &self.edges[producer] {
                    if let Some(value) = degree.get_mut(&consumer) {
                        *value -= 1;
                        if *value == 0 {
                            ready.push_back(consumer);
                        }
                    }
                }
            }
        }

        // If there is a cyclic dependency, some evaluators will never reach degree 0 and will not be visited.
        if visited != required_producers.len() {
            return Err("Cyclic dependency detected in selected evaluators graph".into());
        }
        Ok(stages)
    }
}

#[cfg(test)]
mod tests;
