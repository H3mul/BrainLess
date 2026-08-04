Metric Engine Framework Refactoring

To isolate the engine into a reusable library, we must split the codebase into two distinct domains: the Framework (Engine) and the Implementation (Domain App).

1. Workspace Structure

We will transition to a standard Rust workspace with two crates:

/workspace
 ├── /neuro_engine (The isolated framework crate)
 │    ├── src/
 │    │    ├── lib.rs
 │    │    ├── core/      (Traits: Metric, Evaluator, Dependency DSL)
 │    │    ├── dag/       (DagCompiler, Kahn's algorithm)
 │    │    ├── storage/   (Ring Buffers, Ledger, StorageBackend trait)
 │    │    └── engine.rs  (MetricEngine orchestrator & Builder)
 │
 └── /my_bci_app (The user application)
      ├── src/
      │    ├── metrics.rs     (RawEeg, FAA, Volatility + TsdbStorage impls)
      │    ├── evaluators.rs  (Logic blocks)
      │    ├── duckdb_impl.rs (Implements the engine's StorageBackend trait)
      │    └── main.rs        (App initialization and event loop)


2. Key Abstractions Needed

To make neuro_engine completely agnostic, we need to abstract three hardcoded areas:

A. The Storage Backend Trait

The engine should not know what DuckDB is. Instead, it defines a trait for persistence that the user application implements.

// Inside `neuro_engine::storage::backend`
pub trait StorageBackend: Send + Sync {
    /// Flush a batch of pre-formatted SQL/String rows to the database
    fn flush_batch(&mut self, table_name: &str, schema_columns: &[&str], row_values: &[String]) -> Result<(), String>;
    
    /// Fetch historic data to fulfill DAG dependencies during initialization
    fn fetch_historic(&self, table_name: &str, window_ms: i64) -> Result<Vec<String>, String>;
}


B. The Engine Builder & Evaluator Registry

Instead of the engine holding default_evaluators(), the user registers their evaluators via a builder pattern.

// Inside `neuro_engine::engine`
pub struct MetricEngineBuilder {
    evaluators: Vec<Box<dyn ErasedEvaluator>>,
    target_metrics: Vec<TypeId>,
    storage_backend: Option<Box<dyn StorageBackend>>,
}

impl MetricEngineBuilder {
    pub fn new() -> Self { ... }
    
    pub fn register_evaluator<E: MetricEvaluator + 'static>(mut self, evaluator: E) -> Self {
        self.evaluators.push(Box::new(EvaluatorWrapper::new(evaluator)));
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

    /// Compiles the DAG, allocates buffers, and returns the runnable engine
    pub fn build(self) -> Result<MetricEngine, String> {
        // 1. Run DagCompiler::compile()
        // 2. Provision StorageEngine using the provided backend
        // 3. Return initialized MetricEngine
    }
}


3. The Developer Experience (User Application)

With the engine isolated, the user's main.rs becomes clean, declarative, and focused entirely on their domain.

// Inside `my_bci_app/src/main.rs`
use neuro_engine::{MetricEngine, MetricDependency};
use crate::metrics::{RawEegMetric, FaaMetric, FaaVolatilityMetric};
use crate::evaluators::{FaaEvaluator, FaaVolatilityEvaluator};
use crate::duckdb_impl::MyDuckDbBackend;

fn main() {
    // 1. Setup the Database Connection
    let db_backend = MyDuckDbBackend::new("./sessions.duckdb");

    // 2. Build and compile the engine DAG
    let mut engine = MetricEngine::builder()
        .register_evaluator(FaaEvaluator)
        .register_evaluator(FaaVolatilityEvaluator)
        .with_storage(db_backend)
        .with_targets(vec![
            std::any::TypeId::of::<FaaVolatilityMetric>()
        ])
        .build()
        .expect("Failed to compile DAG");

    // 3. Event Loop (Live Mode)
    loop {
        // Ingest hardware data
        let raw_samples = get_bluetooth_eeg_data();
        
        for sample in raw_samples {
            engine.feed_external(sample);
        }

        // Tick at 1Hz
        if should_tick() {
            let outputs = engine.tick().unwrap();
            
            // Extract UI targets safely
            if let Some(volatility) = outputs.get_latest::<FaaVolatilityMetric>() {
                println!("Current Volatility: {}", volatility.volatility);
            }
        }
    }
}


4. Live vs. Offline Parity

By hiding everything behind feed_external() and tick(), offline batch processing becomes trivial.

To replay a historic session:

The user fetches rows from their TSDB or a CSV file.

They map rows to ExternalMetric instances.

Instead of sleeping for 1 second between tick() calls, they loop continuously: engine.feed_external(...) followed by engine.tick().

The engine processes months of data in seconds, automatically writing newly derived experimental metrics back to the TSDB using the exact same DAG execution path as a live session.
