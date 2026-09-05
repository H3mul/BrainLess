use crate::engine::buffer_store::ReadOnlyBufferStore;
use crate::engine::core::{MetricDependency, MetricId};
use crate::execution::{ErasedEvaluator, ExecutionPlan, ExecutionStage};

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Immutable evaluator dependency graph.
///
/// The graph can be queried repeatedly with different source and target
/// availability sets without rebuilding evaluator relationships.
pub struct DependencyGraph {
    // Full set of available evaluators (and evaluator index ground truth)
    evaluators: Vec<Arc<dyn ErasedEvaluator>>,

    // map of metric type to the index of the evaluator that produces it
    producers: HashMap<MetricId, usize>,
    // Map of evaluator index to list of evaluator indexes that depend on it (Producer -> Consumers)
    edges: Vec<Vec<usize>>,
}

impl DependencyGraph {
    /// Builds the local producer-consumer graph
    ///
    /// Graph nodes are evaluators. An edge connects a producer to a consumer
    /// when the consumer depends on a metric produced by the producer.
    pub fn new(evaluators: Vec<Arc<dyn ErasedEvaluator>>) -> Result<Self, String> {
        // Map of metric type to the index of the evaluator that produces it
        let mut producers = HashMap::new();
        for (index, evaluator) in evaluators.iter().enumerate() {
            for metric_id in evaluator.produces() {
                if producers.insert(metric_id, index).is_some() {
                    return Err(format!(
                        "Multiple evaluators produce metric type {:?}",
                        metric_id
                    ));
                }
            }
        }

        // Graph edges: Producer -> Consumers
        let mut edges = vec![Vec::new(); evaluators.len()];
        for (consumer, evaluator) in evaluators.iter().enumerate() {
            for dependency in evaluator.dependencies() {
                if let Some(&producer) = producers.get(&dependency.metric_id) {
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
    ///
    /// The graph is reusable: evaluators are shared via `Arc` clones, so
    /// multiple plans can be built from the same graph.
    pub fn build_execution_plan(
        &self,
        source_metrics: &HashSet<MetricId>,
        target_metrics: &HashSet<MetricId>,
    ) -> Result<ExecutionPlan, String> {
        // Set of evaluator indexes that are required to produce the requested target metrics
        let mut required_producers = HashSet::new();

        // Set of metric types that have been visited during the traversal
        let mut visited = HashSet::new();
        // Operational queue of metric types to figure out producers for. Start with the requested target metrics and work backwards to their dependencies.
        let mut work: Vec<MetricId> = target_metrics.iter().copied().collect();
        while let Some(metric_id) = work.pop() {
            // Already accounted for this metric's producers, or it's a source metric
            // Note: this means if a metric is in both in sources and targets = no action.
            if !visited.insert(metric_id) || source_metrics.contains(&metric_id) {
                continue;
            }

            // Fetch metric's producer and add it to the required set
            let evaluator_index = self.producers.get(&metric_id).copied()
                .ok_or_else(|| format!("Metric type {:?} is required but has no evaluator producer and was not declared as a source", metric_id))?;

            if required_producers.insert(evaluator_index) {
                // If this is a newly discovered required producer, add its dependencies to the work queue to be resolved
                work.extend(
                    self.evaluators[evaluator_index]
                        .dependencies()
                        .into_iter()
                        .map(|dependency| dependency.metric_id),
                );
            }
        }

        let stages = self.build_stages(&required_producers)?;

        // Aggregate dependency declarations across required producers, used
        // for buffer sizing.
        let aggregate_demand: HashSet<MetricDependency> = required_producers
            .iter()
            .flat_map(|&index| self.evaluators[index].dependencies())
            .collect();

        // Every metric the plan touches: externally fed sources plus all
        // metrics produced by the planned evaluators.
        let mut plan_metrics = source_metrics.clone();
        for &index in &required_producers {
            plan_metrics.extend(self.evaluators[index].produces());
        }

        Ok(ExecutionPlan {
            stages,
            plan_metrics,
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

/// Filters out evaluators that produce metrics already present in the store
/// for this timestamp - we don't need to re-compute them.
///
/// A metric counts as present when its buffer holds a sample within the
/// metric's sample-rate tolerance of `timestamp_ms`. Evaluators producing at
/// least one missing metric are kept whole; stage ordering is preserved.
pub fn optimize_execution_plan_by_metric_existence(
    plan: &mut ExecutionPlan,
    timestamp_ms: i64,
    store: &ReadOnlyBufferStore,
) {
    for stage in plan.stages.iter_mut() {
        stage.evaluators.retain(|evaluator| {
            !evaluator
                .produces()
                .iter()
                .all(|metric_id| store.has_value_at(*metric_id, timestamp_ms))
        });
    }
    plan.stages.retain(|stage| !stage.evaluators.is_empty());
}

#[cfg(test)]
mod tests;
