use crate::core::{Age, AgeRange, ErasedEvaluator, MetricDependency, TickLedger};
use crate::storage::StorageEngine;
use std::any::TypeId;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, info, warn};

/// Execution strategy for a compiled evaluator plan.
pub enum ExecutionMode {
    Sequential,
    Parallel,
}

/// Topologically ordered evaluator sequence for one engine tick.
pub struct ExecutionPlan {
    sequence: Vec<Box<dyn ErasedEvaluator>>,
    mode: ExecutionMode,
}

impl ExecutionPlan {
    /// Creates an execution plan from an ordered evaluator sequence.
    pub fn new(sequence: Vec<Box<dyn ErasedEvaluator>>, mode: ExecutionMode) -> Self {
        Self { sequence, mode }
    }

    /// Runs evaluators and refreshes produced series into the tick ledger.
    pub fn execute(
        &self,
        ledger: &mut TickLedger,
        storage: &mut StorageEngine,
        timestamp_ms: i64,
    ) -> Result<(), String> {
        debug!(
            evaluator_count = self.sequence.len(),
            timestamp_ms, "executing metric evaluation plan"
        );
        match self.mode {
            ExecutionMode::Sequential => {
                for e in &self.sequence {
                    debug!(evaluator = e.id(), timestamp_ms, "evaluating metric");
                    e.evaluate_and_commit(ledger, storage, timestamp_ms)?;

                    for type_id in e.produces() {
                        if let Some(series) = storage.series_erased(type_id) {
                            ledger.insert_erased(type_id, series);
                        }
                    }
                }
            }
            ExecutionMode::Parallel => {
                warn!("parallel metric evaluation was requested but is not implemented");
                return Err("Parallel execution is not implemented".into());
            }
        }

        Ok(())
    }
}

/// Buffers and execution plan produced by DAG compilation.
pub struct CompiledSessionResources {
    pub execution_plan: ExecutionPlan,
    pub ram_buffer_capacities: HashMap<TypeId, usize>,
    pub aggregate_demand: HashSet<MetricDependency>,
}

/// Selects required evaluators and topologically sorts their dependencies.
pub struct DagCompiler;

impl DagCompiler {
    pub fn compile(
        targets: &[TypeId],
        evaluators: Vec<Box<dyn ErasedEvaluator>>,
        mode: ExecutionMode,
    ) -> Result<CompiledSessionResources, String> {
        info!(
            target_count = targets.len(),
            evaluator_count = evaluators.len(),
            "compiling metric dependency graph"
        );
        let mut producers = HashMap::new();

        for (i, e) in evaluators.iter().enumerate() {
            for ty in e.produces() {
                if producers.insert(ty, i).is_some() {
                    let error = format!("Multiple evaluators produce metric type {:?}", ty);
                    warn!(%error, "metric dependency graph has duplicate producer");
                    return Err(error);
                }
            }
        }

        let mut selected = HashSet::new();
        let mut work = targets.to_vec();
        let mut visited = HashSet::new();
        while let Some(ty) = work.pop() {
            if !visited.insert(ty) {
                continue;
            }

            if let Some(&i) = producers.get(&ty) {
                if selected.insert(i) {
                    work.extend(evaluators[i].dependencies().into_iter().map(|d| d.type_id));
                }
            }
        }

        let mut nodes: Vec<Option<Box<dyn ErasedEvaluator>>> =
            evaluators.into_iter().map(Some).collect();

        let chosen: Vec<_> = selected
            .into_iter()
            .map(|i| nodes[i].take().unwrap())
            .collect();

        let n = chosen.len();

        let mut local = HashMap::new();

        for (i, e) in chosen.iter().enumerate() {
            for ty in e.produces() {
                local.insert(ty, i);
            }
        }

        let mut edges = vec![Vec::new(); n];

        let mut degree = vec![0usize; n];

        for (consumer, e) in chosen.iter().enumerate() {
            for dep in e.dependencies() {
                if let Some(&producer) = local.get(&dep.type_id) {
                    if producer != consumer && !edges[producer].contains(&consumer) {
                        edges[producer].push(consumer);

                        degree[consumer] += 1;
                    }
                }
            }
        }

        let mut q = VecDeque::new();
        for (i, d) in degree.iter().enumerate() {
            if *d == 0 {
                q.push_back(i)
            }
        }

        let mut order = Vec::new();
        while let Some(i) = q.pop_front() {
            order.push(i);

            for &next in &edges[i] {
                degree[next] -= 1;
                if degree[next] == 0 {
                    q.push_back(next)
                }
            }
        }

        if order.len() != n {
            let error = "Cyclic dependency detected in selected evaluators graph";
            warn!(%error, selected_evaluator_count = n, "metric dependency graph contains a cycle");
            return Err(error.into());
        }

        let mut ordered: Vec<Option<Box<dyn ErasedEvaluator>>> =
            chosen.into_iter().map(Some).collect();

        let sequence: Vec<Box<dyn ErasedEvaluator>> = order
            .into_iter()
            .map(|i| ordered[i].take().unwrap())
            .collect();

        let mut caps: HashMap<TypeId, usize> = HashMap::new();

        let mut demand: HashSet<MetricDependency> = HashSet::new();

        for e in &sequence {
            for dep in e.dependencies() {
                let cap = match &dep.range {
                    AgeRange::Single(Age::SecondsAgo(s)) => *s,
                    AgeRange::Single(Age::Latest) => 1,
                    AgeRange::Window {
                        start: Age::SecondsAgo(s),
                        ..
                    } => *s,
                    _ => 1,
                };

                demand.insert(dep.clone());

                caps.entry(dep.type_id)
                    .and_modify(|x| *x = (*x).max(cap))
                    .or_insert(cap.max(1));
            }
        }

        info!(
            selected_evaluator_count = sequence.len(),
            dependency_count = demand.len(),
            buffer_count = caps.len(),
            "metric dependency graph compiled"
        );
        Ok(CompiledSessionResources {
            execution_plan: ExecutionPlan::new(sequence, mode),
            ram_buffer_capacities: caps,
            aggregate_demand: demand,
        })
    }
}
