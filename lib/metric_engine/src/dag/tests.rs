use super::*;
use crate::core::Metric;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct SourceMetric;
impl Metric for SourceMetric {}

#[derive(Clone, Debug)]
struct IntermediateMetric;
impl Metric for IntermediateMetric {}

#[derive(Clone, Debug)]
struct TargetMetric;
impl Metric for TargetMetric {}

#[derive(Clone, Debug)]
struct OtherSourceMetric;
impl Metric for OtherSourceMetric {}

struct MockEvaluator {
    name: &'static str,
    output: TypeId,
    dependencies: HashSet<MetricDependency>,
}

impl ErasedEvaluator for MockEvaluator {
    fn id(&self) -> &'static str {
        self.name
    }

    fn produces(&self) -> HashSet<TypeId> {
        HashSet::from([self.output])
    }

    fn dependencies(&self) -> HashSet<MetricDependency> {
        self.dependencies.clone()
    }

    fn evaluate_and_commit(
        &self,
        _ledger: &TickLedger,
        _storage: &mut StorageEngine,
        _timestamp_ms: i64,
    ) -> Result<(), String> {
        Ok(())
    }
}

fn evaluator(
    name: &'static str,
    output: TypeId,
    dependencies: impl IntoIterator<Item = MetricDependency>,
) -> Arc<dyn ErasedEvaluator> {
    Arc::new(MockEvaluator {
        name,
        output,
        dependencies: dependencies.into_iter().collect(),
    })
}

fn graph(evaluators: impl IntoIterator<Item = Arc<dyn ErasedEvaluator>>) -> DagGraph {
    DagGraph::new(evaluators.into_iter().collect()).expect("test graph should compile")
}

#[test]
fn builds_independent_evaluators_into_one_stage() {
    let dag = graph([
        evaluator("source_a", TypeId::of::<SourceMetric>(), []),
        evaluator("source_b", TypeId::of::<OtherSourceMetric>(), []),
    ]);
    let plan = dag
        .traverse(
            &HashSet::new(),
            &HashSet::from([
                TypeId::of::<SourceMetric>(),
                TypeId::of::<OtherSourceMetric>(),
            ]),
            ExecutionMode::Sequential,
        )
        .unwrap()
        .execution_plan;

    assert_eq!(plan.stages.len(), 1);
    assert_eq!(plan.stages[0].evaluators.len(), 2);
}

#[test]
fn orders_consumers_after_all_local_producers() {
    let dag = graph([
        evaluator("source", TypeId::of::<SourceMetric>(), []),
        evaluator(
            "intermediate",
            TypeId::of::<IntermediateMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
        evaluator(
            "target",
            TypeId::of::<TargetMetric>(),
            [MetricDependency::latest::<IntermediateMetric>()],
        ),
    ]);
    let plan = dag
        .traverse(
            &HashSet::new(),
            &HashSet::from([TypeId::of::<TargetMetric>()]),
            ExecutionMode::Sequential,
        )
        .unwrap()
        .execution_plan;

    assert_eq!(plan.stages.len(), 3);
    assert_eq!(plan.stages[0].evaluators[0].id(), "source");
    assert_eq!(plan.stages[1].evaluators[0].id(), "intermediate");
    assert_eq!(plan.stages[2].evaluators[0].id(), "target");
}

#[test]
fn source_metrics_stop_backwards_producer_selection() {
    let dag = graph([
        evaluator("source_producer", TypeId::of::<SourceMetric>(), []),
        evaluator(
            "target",
            TypeId::of::<TargetMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let plan = dag
        .traverse(
            &HashSet::from([TypeId::of::<SourceMetric>()]),
            &HashSet::from([TypeId::of::<TargetMetric>()]),
            ExecutionMode::Sequential,
        )
        .unwrap()
        .execution_plan;

    assert_eq!(plan.stages.len(), 1);
    assert_eq!(plan.stages[0].evaluators[0].id(), "target");
}

#[test]
fn traversal_can_be_repeated_with_different_sources() {
    let dag = graph([
        evaluator("source", TypeId::of::<SourceMetric>(), []),
        evaluator(
            "target",
            TypeId::of::<TargetMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let without_source = dag
        .traverse(
            &HashSet::new(),
            &HashSet::from([TypeId::of::<TargetMetric>()]),
            ExecutionMode::Sequential,
        )
        .unwrap();
    let with_source = dag
        .traverse(
            &HashSet::from([TypeId::of::<SourceMetric>()]),
            &HashSet::from([TypeId::of::<TargetMetric>()]),
            ExecutionMode::Sequential,
        )
        .unwrap();

    assert_eq!(without_source.execution_plan.stages.len(), 2);
    assert_eq!(with_source.execution_plan.stages.len(), 1);
    assert_eq!(with_source.aggregate_demand.len(), 1);
}

#[test]
fn rejects_unresolved_dependencies() {
    let dag = graph([evaluator(
        "target",
        TypeId::of::<TargetMetric>(),
        [MetricDependency::latest::<SourceMetric>()],
    )]);
    let error = dag
        .traverse(
            &HashSet::new(),
            &HashSet::from([TypeId::of::<TargetMetric>()]),
            ExecutionMode::Sequential,
        )
        .err()
        .expect("expected error");

    assert!(error.contains("no evaluator producer"));
}

#[test]
fn rejects_duplicate_metric_producers() {
    let result = DagGraph::new(vec![
        evaluator("first", TypeId::of::<TargetMetric>(), []),
        evaluator("second", TypeId::of::<TargetMetric>(), []),
    ]);

    assert!(result.is_err());
}

#[test]
fn rejects_cycles_during_traversal() {
    let dag = graph([
        evaluator(
            "a",
            TypeId::of::<SourceMetric>(),
            [MetricDependency::latest::<IntermediateMetric>()],
        ),
        evaluator(
            "b",
            TypeId::of::<IntermediateMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let error = dag
        .traverse(
            &HashSet::new(),
            &HashSet::from([TypeId::of::<SourceMetric>()]),
            ExecutionMode::Sequential,
        )
        .err()
        .expect("expected error");

    assert!(error.contains("Cyclic dependency"));
}
