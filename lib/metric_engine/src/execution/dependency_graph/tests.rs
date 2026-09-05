use super::*;
use crate::execution::StagedOutput;
use crate::{BufferStore, Metric, MetricDependency, MetricId};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct SourceMetric;
impl Metric for SourceMetric {
    fn id() -> &'static str {
        "source_metric"
    }
}

#[derive(Clone, Debug)]
struct IntermediateMetric;
impl Metric for IntermediateMetric {
    fn id() -> &'static str {
        "intermediate_metric"
    }
}

#[derive(Clone, Debug)]
struct TargetMetric;
impl Metric for TargetMetric {
    fn id() -> &'static str {
        "target_metric"
    }
}

#[derive(Clone, Debug)]
struct OtherSourceMetric;
impl Metric for OtherSourceMetric {
    fn id() -> &'static str {
        "other_source_metric"
    }
}

struct NoopOutput;
impl StagedOutput for NoopOutput {
    fn commit_to_store(self: Box<Self>, _timestamp_ms: i64, _store: &mut BufferStore) {}
}

struct MockEvaluator {
    name: &'static str,
    output: MetricId,
    dependencies: HashSet<MetricDependency>,
}

impl ErasedEvaluator for MockEvaluator {
    fn id(&self) -> &'static str {
        self.name
    }

    fn produces(&self) -> HashSet<MetricId> {
        HashSet::from([self.output])
    }

    fn dependencies(&self) -> HashSet<MetricDependency> {
        self.dependencies.clone()
    }

    fn evaluate_erased(
        &self,
        _timestamp_ms: i64,
        _store: &ReadOnlyBufferStore,
    ) -> Result<Box<dyn StagedOutput>, String> {
        Ok(Box::new(NoopOutput))
    }
}

fn evaluator(
    name: &'static str,
    output: MetricId,
    dependencies: impl IntoIterator<Item = MetricDependency>,
) -> Arc<dyn ErasedEvaluator> {
    Arc::new(MockEvaluator {
        name,
        output,
        dependencies: dependencies.into_iter().collect(),
    })
}

fn graph(evaluators: impl IntoIterator<Item = Arc<dyn ErasedEvaluator>>) -> DependencyGraph {
    DependencyGraph::new(evaluators.into_iter().collect()).expect("test graph should compile")
}

#[test]
fn builds_independent_evaluators_into_one_stage() {
    let dag = graph([
        evaluator("source_a", MetricId::of::<SourceMetric>(), []),
        evaluator("source_b", MetricId::of::<OtherSourceMetric>(), []),
    ]);
    let plan = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([
                MetricId::of::<SourceMetric>(),
                MetricId::of::<OtherSourceMetric>(),
            ]),
        )
        .unwrap();

    assert_eq!(plan.stages.len(), 1);
    assert_eq!(plan.stages[0].evaluators.len(), 2);
}

#[test]
fn orders_consumers_after_all_local_producers() {
    let dag = graph([
        evaluator("source", MetricId::of::<SourceMetric>(), []),
        evaluator(
            "intermediate",
            MetricId::of::<IntermediateMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
        evaluator(
            "target",
            MetricId::of::<TargetMetric>(),
            [MetricDependency::latest::<IntermediateMetric>()],
        ),
    ]);
    let plan = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .unwrap();

    assert_eq!(plan.stages.len(), 3);
    assert_eq!(plan.stages[0].evaluators[0].id(), "source");
    assert_eq!(plan.stages[1].evaluators[0].id(), "intermediate");
    assert_eq!(plan.stages[2].evaluators[0].id(), "target");
}

#[test]
fn source_metrics_stop_backwards_producer_selection() {
    let dag = graph([
        evaluator("source_producer", MetricId::of::<SourceMetric>(), []),
        evaluator(
            "target",
            MetricId::of::<TargetMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let plan = dag
        .build_execution_plan(
            &HashSet::from([MetricId::of::<SourceMetric>()]),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .unwrap();

    assert_eq!(plan.stages.len(), 1);
    assert_eq!(plan.stages[0].evaluators[0].id(), "target");
    assert_eq!(
        plan.plan_metrics,
        HashSet::from([
            MetricId::of::<SourceMetric>(),
            MetricId::of::<TargetMetric>()
        ])
    );
}

#[test]
fn traversal_can_be_repeated_with_different_sources() {
    let dag = graph([
        evaluator("source", MetricId::of::<SourceMetric>(), []),
        evaluator(
            "target",
            MetricId::of::<TargetMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let without_source = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .unwrap();
    let with_source = dag
        .build_execution_plan(
            &HashSet::from([MetricId::of::<SourceMetric>()]),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .unwrap();

    assert_eq!(without_source.stages.len(), 2);
    assert_eq!(with_source.stages.len(), 1);
    assert_eq!(without_source.aggregate_demand.len(), 1);
    assert_eq!(with_source.aggregate_demand.len(), 1);
}

#[test]
fn plan_collects_metrics_and_aggregate_demand() {
    let dag = graph([
        evaluator("source", MetricId::of::<SourceMetric>(), []),
        evaluator(
            "intermediate",
            MetricId::of::<IntermediateMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
        evaluator(
            "target",
            MetricId::of::<TargetMetric>(),
            [MetricDependency::latest::<IntermediateMetric>()],
        ),
    ]);
    let plan = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .unwrap();

    assert_eq!(
        plan.plan_metrics,
        HashSet::from([
            MetricId::of::<SourceMetric>(),
            MetricId::of::<IntermediateMetric>(),
            MetricId::of::<TargetMetric>(),
        ])
    );
    assert_eq!(
        plan.aggregate_demand,
        HashSet::from([
            MetricDependency::latest::<SourceMetric>(),
            MetricDependency::latest::<IntermediateMetric>(),
        ])
    );
}

#[test]
fn rejects_unresolved_dependencies() {
    let dag = graph([evaluator(
        "target",
        MetricId::of::<TargetMetric>(),
        [MetricDependency::latest::<SourceMetric>()],
    )]);
    let error = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([MetricId::of::<TargetMetric>()]),
        )
        .err()
        .expect("expected error");

    assert!(error.contains("no evaluator producer"));
}

#[test]
fn rejects_duplicate_metric_producers() {
    let result = DependencyGraph::new(vec![
        evaluator("first", MetricId::of::<TargetMetric>(), []),
        evaluator("second", MetricId::of::<TargetMetric>(), []),
    ]);

    assert!(result.is_err());
}

#[test]
fn rejects_cycles_during_traversal() {
    let dag = graph([
        evaluator(
            "a",
            MetricId::of::<SourceMetric>(),
            [MetricDependency::latest::<IntermediateMetric>()],
        ),
        evaluator(
            "b",
            MetricId::of::<IntermediateMetric>(),
            [MetricDependency::latest::<SourceMetric>()],
        ),
    ]);
    let error = dag
        .build_execution_plan(
            &HashSet::new(),
            &HashSet::from([MetricId::of::<SourceMetric>()]),
        )
        .err()
        .expect("expected error");

    assert!(error.contains("Cyclic dependency"));
}
