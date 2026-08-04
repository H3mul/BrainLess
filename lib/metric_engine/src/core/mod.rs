use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet, VecDeque};

/// Base trait implemented by every value that flows through the metric engine.
///
/// Metrics must be cloneable because the engine keeps bounded historical copies
/// in its time-series buffers and passes read-only snapshots to evaluators.
pub trait Metric: Send + Sync + Clone + 'static {}

/// Marker trait for metrics originating outside the DAG execution pipeline,
/// such as hardware sensors, raw EEG streams, or external API feeds.
pub trait ExternalMetric: Metric {}

/// Application-provided persistence mapping for a metric type.
///
/// The engine uses this abstraction to hand formatted rows to a storage
/// backend without knowing the database schema or SQL dialect.
pub trait TsdbStorage: Metric {
    /// Target table name for this metric model.
    fn table_name() -> &'static str;
    /// Column names passed to the storage backend.
    fn schema_columns() -> &'static [&'static str];
    /// DDL statement used to create the metric table.
    fn create_table_sql() -> &'static str;
    /// Parameterized insert statement used for batch flushing.
    fn insert_sql() -> &'static str;
    /// Query used to fetch a historic metric range.
    fn select_range_sql() -> &'static str;
    /// Formats metric values into backend row parameters.
    fn to_sql_params(&self) -> Vec<String>;
    /// Deserializes a backend row into the typed metric value.
    fn from_sql_row(row_params: &[&str]) -> Result<Self, String>;
}

/// Sampling policy requested by a window dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SampleRate {
    Best,
    Hz(u32),
}

/// Relative age of a requested metric sample.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Age {
    Latest,
    SecondsAgo(usize),
}

/// A single point or historical window requested from a metric buffer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AgeRange {
    Single(Age),
    Window {
        start: Age,
        end: Age,
        rate: SampleRate,
    },
}

/// Declarative dependency request for a specific metric type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricDependency {
    pub type_id: TypeId,
    pub range: AgeRange,
}

impl MetricDependency {
    /// Requests the newest available sample for `T`.
    pub fn latest<T: Metric>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            range: AgeRange::Single(Age::Latest),
        }
    }

    /// Requests a bounded historical window for `T`, expressed in seconds ago.
    pub fn window<T: Metric>(start_secs: usize, end_secs: usize, rate: SampleRate) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            range: AgeRange::Window {
                start: Age::SecondsAgo(start_secs),
                end: Age::SecondsAgo(end_secs),
                rate,
            },
        }
    }
}

/// A metric value paired with its source timestamp.
#[derive(Debug, Clone)]
pub struct MetricSample<T: Metric> {
    pub timestamp_ms: i64,
    pub data: T,
}

/// Bounded double-ended ring buffer for live samples and historic merges.
///
/// Pushes and front eviction are O(1); historic merges are sorted and
/// deduplicated by timestamp before the capacity limit is applied.
#[derive(Debug, Clone)]
pub struct TimeSeriesBuffer<T: Metric> {
    pub samples: VecDeque<MetricSample<T>>,
    pub capacity: usize,
}

impl<T: Metric> TimeSeriesBuffer<T> {
    /// Creates a buffer with a minimum effective capacity of one sample.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
        }
    }

    /// Appends a value, evicting the oldest sample when the buffer is full.
    pub fn push_latest(&mut self, timestamp_ms: i64, data: T) {
        self.push_sample(MetricSample { timestamp_ms, data });
    }

    /// Appends an already timestamped sample to the buffer.
    pub fn push_sample(&mut self, sample: MetricSample<T>) {
        if self.samples.len() == self.capacity {
            self.samples.pop_front();
        }

        self.samples.push_back(sample);
    }

    /// Merges historic samples, sorting and deduplicating by timestamp.
    pub fn merge_historic(&mut self, historic: Vec<MetricSample<T>>) {
        let mut all: Vec<_> = self.samples.drain(..).chain(historic).collect();
        all.sort_by_key(|s| s.timestamp_ms);
        all.dedup_by_key(|s| s.timestamp_ms);
        let start = all.len().saturating_sub(self.capacity);
        self.samples = all[start..].iter().cloned().collect();
    }

    /// Returns the newest sample, if one is available.
    pub fn latest(&self) -> Option<&MetricSample<T>> {
        self.samples.back()
    }

    /// Returns the underlying deque for read-only iteration.
    pub fn as_slice(&self) -> &VecDeque<MetricSample<T>> {
        &self.samples
    }

    /// Returns the number of samples currently retained.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Returns whether the buffer contains no samples.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

/// Read-pass snapshot shared by evaluators during one engine tick.
#[derive(Default)]
pub struct TickLedger {
    pub timestamp_ms: i64,
    store: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl TickLedger {
    /// Creates an empty ledger for a tick timestamp.
    pub fn new(timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms,
            store: HashMap::new(),
        }
    }

    /// Inserts a typed series into the type-erased ledger.
    pub fn insert_series<T: Metric>(&mut self, series: TimeSeriesBuffer<T>) {
        self.store.insert(TypeId::of::<T>(), Box::new(series));
    }

    /// Inserts a series whose concrete type is already erased.
    pub fn insert_erased(&mut self, id: TypeId, series: Box<dyn Any + Send + Sync>) {
        self.store.insert(id, series);
    }

    /// Retrieves the typed series for `T`, or an explanatory missing-type error.
    pub fn get<T: Metric>(&self) -> Result<&TimeSeriesBuffer<T>, String> {
        self.store
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
            .ok_or_else(|| format!("Metric type {:?} missing from ledger", TypeId::of::<T>()))
    }
}

/// Output shape supported by an evaluator.
///
/// Implementations commit one or more typed metric values to storage after
/// evaluation completes.
pub trait MetricGroup: Send + Sync + 'static {
    /// Returns the concrete metric types contained in this output.
    fn type_ids() -> HashSet<TypeId>;

    /// Commits the output values to the engine's storage buffers.
    fn commit_to_storage(self, storage: &mut crate::storage::StorageEngine, timestamp_ms: i64);
}

impl<T: Metric> MetricGroup for T {
    fn type_ids() -> HashSet<TypeId> {
        HashSet::from([TypeId::of::<T>()])
    }

    fn commit_to_storage(self, storage: &mut crate::storage::StorageEngine, timestamp_ms: i64) {
        storage.commit_sample(MetricSample {
            timestamp_ms,
            data: self,
        });
    }
}

impl<A: Metric, B: Metric> MetricGroup for (A, B) {
    fn type_ids() -> HashSet<TypeId> {
        HashSet::from([TypeId::of::<A>(), TypeId::of::<B>()])
    }

    fn commit_to_storage(self, storage: &mut crate::storage::StorageEngine, timestamp_ms: i64) {
        storage.commit_sample(MetricSample {
            timestamp_ms,
            data: self.0,
        });

        storage.commit_sample(MetricSample {
            timestamp_ms,
            data: self.1,
        });
    }
}

/// Typed evaluator implementation supplied by an application crate.
pub trait MetricEvaluator: Send + Sync {
    /// Metric value or tuple produced by this evaluator.
    type Output: MetricGroup;

    /// Stable identifier used in diagnostics and tracing.
    fn id(&self) -> &'static str;

    /// Dependencies required before evaluation can run.
    fn dependencies(&self) -> HashSet<MetricDependency>;

    /// Evaluates against the read-only tick ledger.
    fn evaluate(&self, ledger: &TickLedger) -> Result<Self::Output, String>;
}

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
        storage: &mut crate::storage::StorageEngine,
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
        storage: &mut crate::storage::StorageEngine,
        timestamp_ms: i64,
    ) -> Result<(), String> {
        self.evaluate(ledger)?
            .commit_to_storage(storage, timestamp_ms);

        Ok(())
    }
}
