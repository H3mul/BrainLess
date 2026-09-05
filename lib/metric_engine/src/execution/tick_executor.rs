use crate::{BufferStore, engine::buffer_store::ReadOnlyBufferStore, execution::ExecutionPlan};

pub struct TickExecutor {
    plan: ExecutionPlan,
}

impl TickExecutor {
    pub fn new(plan: ExecutionPlan) -> Self {
        Self { plan }
    }

    pub fn tick(&self, timestamp_ms: i64, store: &mut BufferStore) {
        for stage in &self.plan.stages {
            let mut stage_outputs = Vec::with_capacity(stage.evaluators.len());

            {
                let read_view = ReadOnlyBufferStore::new(store);

                for evaluator in &stage.evaluators {
                    match evaluator.evaluate_erased(timestamp_ms, &read_view) {
                        Ok(staged_output) => stage_outputs.push(staged_output),
                        Err(err) => {
                            // Diagnostics tracing using evaluator.id()
                            tracing::warn!(evaluator_id = evaluator.id(), %err, "evaluator skipped or failed");
                        }
                    }
                }
            }

            for sample in stage_outputs {
                sample.commit_to_store(timestamp_ms, store);
            }
        }
    }
}
