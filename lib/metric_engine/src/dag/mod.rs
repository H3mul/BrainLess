use crate::core::{MetricDependency, TickLedger};
use crate::storage::StorageEngine;
use crate::{MetricEvaluator, MetricGroup};
use std::any::TypeId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::{debug, info, warn};

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
        storage: &mut StorageEngine,
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
        storage: &mut StorageEngine,
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
    pub fn execute(&self, storage: &mut StorageEngine, timestamp_ms: i64) -> Result<(), String> {
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

/// Fulfillment requirements for a session based on requested target metrics and Evaluator depdency graph
pub struct CompiledSessionResources {
    // Stages of parallel execution of Evaluators.
    pub execution_plan: ExecutionPlan,

    // Set of all MetricsDependencies required by the session to produce requested target metrics
    pub aggregate_demand: HashSet<MetricDependency>,
}

/// Immutable evaluator dependency graph.
///
/// The graph can be queried repeatedly with different source and target
/// availability sets without rebuilding evaluator relationships.
pub struct DagGraph {
    evaluators: Vec<Arc<dyn ErasedEvaluator>>,
    producers: HashMap<TypeId, usize>,
    edges: Vec<Vec<usize>>,
}

impl DagGraph {
    /// Builds and validates the immutable evaluator graph once.
    pub fn new(evaluators: Vec<Arc<dyn ErasedEvaluator>>) -> Result<Self, String> {
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

    /// Returns a plan for the requested targets and currently available sources.
    pub fn traversal(
        &self,
        targets: &HashSet<TypeId>,
        source_metrics: &HashSet<TypeId>,
        mode: ExecutionMode,
    ) -> Result<CompiledSessionResources, String> {
        let mut selected = HashSet::new();
        let mut visited = HashSet::new();
        let mut work: Vec<TypeId> = targets.iter().copied().collect();
        while let Some(metric_type) = work.pop() {
            if !visited.insert(metric_type) || source_metrics.contains(&metric_type) {
                continue;
            }
            let evaluator_index = self.producers.get(&metric_type).copied().ok_or_else(|| format!("Metric type {:?} is required but has no evaluator producer and was not declared as a source", metric_type))?;
            if selected.insert(evaluator_index) {
                work.extend(
                    self.evaluators[evaluator_index]
                        .dependencies()
                        .into_iter()
                        .map(|dependency| dependency.type_id),
                );
            }
        }
        let selected_evaluators: Vec<_> = selected
            .iter()
            .map(|&index| self.evaluators[index].clone())
            .collect();
        let aggregate_demand = DagCompiler::calculate_resources(&selected_evaluators);
        let stages = self.build_stages(&selected)?;
        Ok(CompiledSessionResources {
            execution_plan: ExecutionPlan { stages, mode },
            aggregate_demand,
        })
    }

    fn build_stages(&self, selected: &HashSet<usize>) -> Result<Vec<ExecutionStage>, String> {
        let mut degree: HashMap<usize, usize> = selected.iter().map(|&index| (index, 0)).collect();
        for &producer in selected {
            for &consumer in &self.edges[producer] {
                if selected.contains(&consumer) {
                    *degree.get_mut(&consumer).unwrap() += 1;
                }
            }
        }
        let mut ready: VecDeque<_> = degree
            .iter()
            .filter_map(|(&index, &value)| (value == 0).then_some(index))
            .collect();
        let mut visited = 0;
        let mut stages = Vec::new();
        while !ready.is_empty() {
            let current: Vec<_> = ready.drain(..).collect();
            visited += current.len();
            stages.push(ExecutionStage {
                evaluators: current
                    .iter()
                    .map(|&index| self.evaluators[index].clone())
                    .collect(),
            });
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
        if visited != selected.len() {
            return Err("Cyclic dependency detected in selected evaluators graph".into());
        }
        Ok(stages)
    }
}

/// Selects required evaluators, builds local dependency edges, and creates stages.
pub struct DagCompiler;

impl DagCompiler {
    /// Compiles the evaluator subset required by `targets`.
    ///
    /// Types in `source_metrics` are treated as runtime-provided inputs. Their
    /// evaluators are intentionally not selected, even when a producer exists.
    /// Unresolved types are reported at runtime when an evaluator requests them.
    pub fn compile(
        targets: &HashSet<TypeId>,
        source_metrics: &HashSet<TypeId>,
        available_evaluators: Vec<Arc<dyn ErasedEvaluator>>,
        mode: ExecutionMode,
    ) -> Result<CompiledSessionResources, String> {
        info!(
            target_count = targets.len(),
            source_count = source_metrics.len(),
            evaluator_count = available_evaluators.len(),
            "compiling metric dependency graph"
        );
        let graph = DagGraph::new(available_evaluators)?;
        let resources = graph.traversal(targets, source_metrics, mode)?;
        info!(
            stage_count = resources.execution_plan.stages.len(),
            dependency_count = resources.aggregate_demand.len(),
            "metric dependency graph compiled"
        );
        Ok(resources)
    }

    fn index_producers(
        evaluators: &[Arc<dyn ErasedEvaluator>],
    ) -> Result<HashMap<TypeId, usize>, String> {
        let mut producers = HashMap::new();
        for (index, evaluator) in evaluators.iter().enumerate() {
            for metric_type in evaluator.produces() {
                if producers.insert(metric_type, index).is_some() {
                    let error =
                        format!("Multiple evaluators produce metric type {:?}", metric_type);
                    warn!(%error, "metric dependency graph has duplicate producer");
                    return Err(error);
                }
            }
        }
        Ok(producers)
    }

    fn collect_required_evaluators(
        targets: &HashSet<TypeId>,
        source_metrics: &HashSet<TypeId>,
        producers: &HashMap<TypeId, usize>,
        evaluators: Vec<Arc<dyn ErasedEvaluator>>,
    ) -> Result<Vec<Arc<dyn ErasedEvaluator>>, String> {
        let mut selected = Vec::new();
        let mut selected_set = HashSet::new();
        let mut visited_types = HashSet::new();
        let mut work: Vec<TypeId> = targets.iter().copied().collect();
        while let Some(metric_type) = work.pop() {
            // We already accounted for this metric's producers, or it's a source metric
            if !visited_types.insert(metric_type) || source_metrics.contains(&metric_type) {
                continue;
            }
            // This metric has a producer; grab it and source its dependencies
            if let Some(&evaluator_index) = producers.get(&metric_type) {
                if selected_set.insert(evaluator_index) {
                    selected.push(evaluator_index);
                    // Add this producer's dependencies to metrics yet to be sourced
                    work.extend(
                        evaluators[evaluator_index]
                            .dependencies()
                            .into_iter()
                            .map(|dependency| dependency.type_id),
                    );
                }
            } else {
                let error = format!(
                    "Metric type {:?} is required but has no evaluator producer and was not declared as a source; implement an evaluator for this metric or supply it through the source metrics set",
                    metric_type
                );
                warn!(%error, ?metric_type, "unresolved metric dependency");
                return Err(error);
            }
        }
        Ok(Self::materialize_selected(evaluators, selected))
    }

    fn materialize_selected(
        mut evaluators: Vec<Arc<dyn ErasedEvaluator>>,
        indices: Vec<usize>,
    ) -> Vec<Arc<dyn ErasedEvaluator>> {
        let mut slots: Vec<Option<Arc<dyn ErasedEvaluator>>> =
            evaluators.drain(..).map(Some).collect();
        indices
            .into_iter()
            .map(|index| slots[index].take().unwrap())
            .collect()
    }

    /// Builds the local producer-consumer graph and traverses it into stages.
    ///
    /// Graph nodes are evaluators. An edge connects a producer to a consumer
    /// when the consumer depends on a metric produced by the producer. Source
    /// metrics do not create edges because they are supplied outside the DAG.
    /// The same mutable in-degree state is used to form each independent stage
    /// and advance the traversal to the next dependency layer.
    fn traverse_evaluator_dependencies(
        evaluators: Vec<Arc<dyn ErasedEvaluator>>,
    ) -> Result<Vec<ExecutionStage>, String> {
        // Graph nodes: Evaluators
        let mut producers = HashMap::new();
        for (consumer, evaluator) in evaluators.iter().enumerate() {
            for metric_type in evaluator.produces() {
                producers.insert(metric_type, consumer);
            }
        }

        // Graph edges: Evaluator produces -> Evaluator dependency
        let mut edges = vec![Vec::new(); evaluators.len()];
        // In-degree: counter of edges pointing to a given Evaluator
        //  (the number of its dependencies that are other Evaluators, not source metrics;
        //   a degree of 0 indicates it can be executed immediately)
        let mut in_degree = vec![0; evaluators.len()];

        for (consumer, evaluator) in evaluators.iter().enumerate() {
            for dependency in evaluator.dependencies() {
                if let Some(&producer) = producers.get(&dependency.type_id) {
                    // Self-loops are rejected during evaluator registration.
                    if producer != consumer && !edges[producer].contains(&consumer) {
                        edges[producer].push(consumer);
                        in_degree[consumer] += 1;
                    }
                }
            }
        }

        let count = evaluators.len();
        // Copy of full evaluator list, used to track which evaluators have been executed
        let mut slots: Vec<Option<Arc<dyn ErasedEvaluator>>> =
            evaluators.into_iter().map(Some).collect();

        // Operational queue of evaluators that are ready to execute (degree 0)
        // Degree-zero evaluators have no local evaluator dependencies and can
        // execute immediately. Each ready layer becomes one execution stage.
        let mut ready = VecDeque::new();

        // Initialize ready queue with immediate degree 0 evaluators
        for (index, degree) in in_degree.iter().enumerate() {
            if *degree == 0 {
                ready.push_back(index);
            }
        }

        let mut stages = Vec::new();
        let mut visited = 0;
        while !ready.is_empty() {
            // Flush the ready queue into a stage
            let current: Vec<_> = ready.drain(..).collect();
            visited += current.len();
            let stage_evaluators = current
                .iter()
                .map(|&index| slots[index].take().unwrap())
                .collect();
            stages.push(ExecutionStage {
                evaluators: stage_evaluators,
            });

            // Decrement the current stage's outgoing edges: we have fulfilled these dependencies.
            // Consumers whose in-degree reaches zero become ready for the next stage
            for &producer in &current {
                for &consumer in &edges[producer] {
                    in_degree[consumer] -= 1;
                    if in_degree[consumer] == 0 {
                        ready.push_back(consumer);
                    }
                }
            }
        }

        if visited != count {
            let error = "Cyclic dependency detected in selected evaluators graph";
            warn!(%error, selected_evaluator_count = count, "metric dependency graph contains a cycle");
            return Err(error.into());
        }
        Ok(stages)
    }

    /// Calculates buffer capacities and aggregate dependency demand from selected evaluators.
    fn calculate_resources(evaluators: &[Arc<dyn ErasedEvaluator>]) -> HashSet<MetricDependency> {
        let mut demand: HashSet<MetricDependency> = HashSet::new();
        for evaluator in evaluators {
            for dependency in evaluator.dependencies() {
                demand.insert(dependency.clone());
            }
        }
        demand
    }
}
