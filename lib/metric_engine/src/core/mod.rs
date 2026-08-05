use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;

/// Base trait implemented by every value that flows through the metric engine.
/// Structs implementing this trait are expected to be both the metric type identifier
/// and its data container.
///
/// Metrics must be cloneable because the engine keeps bounded historical copies
/// in its time-series buffers and passes read-only snapshots to evaluators.
pub trait Metric: Send + Sync + Clone + Debug + 'static {}

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

/// A request for a metric sampling - a single datapoint or historical window from a buffer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SampleRequest {
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
    pub request: SampleRequest,
}

impl MetricDependency {
    /// Requests the newest available sample for `T`.
    pub fn latest<T: Metric>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            request: SampleRequest::Single(Age::Latest),
        }
    }

    /// Requests a bounded historical window for `T`, expressed in seconds ago.
    pub fn window<T: Metric>(start_secs: usize, end_secs: usize, rate: SampleRate) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            request: SampleRequest::Window {
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
/// Samples are retained by timestamp age rather than count. Every insertion
/// evicts samples older than `time_capacity_ms` relative to the newest sample.
#[derive(Debug, Clone)]
pub struct TimeSeriesBuffer<T: Metric> {
    pub samples: VecDeque<MetricSample<T>>,
    pub time_capacity_ms: i64,
}

impl<T: Metric> TimeSeriesBuffer<T> {
    /// Creates a time-bounded buffer whose retention window is expressed in milliseconds.
    pub fn with_time_capacity_ms(time_capacity_ms: i64) -> Self {
        Self {
            samples: VecDeque::new(),
            time_capacity_ms: time_capacity_ms.max(1),
        }
    }

    /// Appends a value and evicts samples older than the retention window.
    pub fn push_latest(&mut self, timestamp_ms: i64, data: T) {
        self.push_sample(MetricSample { timestamp_ms, data });
    }

    /// Appends an already timestamped sample and applies age-based eviction.
    pub fn push_sample(&mut self, sample: MetricSample<T>) {
        self.samples.push_back(sample);
        self.evict_old_samples();
    }

    /// Merges historic samples, sorting, deduplicating, and age-pruning by timestamp.
    pub fn merge_historic(&mut self, historic: Vec<MetricSample<T>>) {
        let mut all: Vec<_> = self.samples.drain(..).chain(historic).collect();
        all.sort_by_key(|s| s.timestamp_ms);
        all.dedup_by_key(|s| s.timestamp_ms);
        self.samples = all.into_iter().collect();
        self.evict_old_samples();
    }

    fn evict_old_samples(&mut self) {
        let Some(newest_timestamp_ms) = self.samples.back().map(|sample| sample.timestamp_ms)
        else {
            return;
        };
        let cutoff = newest_timestamp_ms.saturating_sub(self.time_capacity_ms);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.timestamp_ms < cutoff)
        {
            self.samples.pop_front();
        }
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

pub(crate) trait ErasedSeries: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn type_name(&self) -> &'static str;
    fn debug_rows(&self) -> Vec<(i64, String)>;
}

impl<T: Metric> ErasedSeries for TimeSeriesBuffer<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
    fn debug_rows(&self) -> Vec<(i64, String)> {
        self.samples
            .iter()
            .map(|sample| (sample.timestamp_ms, format!("{:?}", sample.data)))
            .collect()
    }
}

/// Read-pass snapshot shared by evaluators during one engine tick.
#[derive(Default)]
pub struct TickLedger {
    pub timestamp_ms: i64,
    store: HashMap<TypeId, Arc<dyn ErasedSeries>>,
}

/// Read-only view of a completed tick ledger exposed to engine callers.
pub struct TickOutputLedger {
    ledger: TickLedger,
}

impl TickOutputLedger {
    pub(crate) fn new(ledger: TickLedger) -> Self {
        Self { ledger }
    }

    /// Timestamp associated with this completed tick.
    pub fn timestamp_ms(&self) -> i64 {
        self.ledger.timestamp_ms
    }

    /// Retrieves a typed metric series without exposing ledger mutation.
    pub fn get<T: Metric>(&self) -> Result<&TimeSeriesBuffer<T>, String> {
        self.ledger.get::<T>()
    }

    /// Pretty-prints every captured metric series with aligned timestamps.
    pub fn pretty_print(&self) -> String {
        self.ledger.pretty_print()
    }
}

impl TickLedger {
    /// Creates an empty ledger for a tick timestamp.
    pub(crate) fn new(timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms,
            store: HashMap::new(),
        }
    }



    /// Inserts a series whose concrete type is already erased.
    pub(crate) fn insert_erased(&mut self, id: TypeId, series: Arc<dyn ErasedSeries>) {
        self.store.insert(id, series);
    }

    /// Retrieves the typed series for `T`, or an explanatory missing-type error.
    /// Pretty-prints all type-erased metric series captured in this ledger.
    pub fn pretty_print(&self) -> String {
        let mut series: Vec<_> = self
            .store
            .iter()
            .map(|(type_id, series)| {
                (
                    format!("{:?} ({})", type_id, series.type_name()),
                    series.debug_rows(),
                )
            })
            .collect();
        series.sort_by(|left, right| left.0.cmp(&right.0));
        let timestamp_width = series
            .iter()
            .flat_map(|(_, rows)| {
                rows.iter()
                    .map(|(timestamp, _)| timestamp.to_string().len())
            })
            .max()
            .unwrap_or(1);
        let mut output = String::new();
        for (index, (title, rows)) in series.iter().enumerate() {
            if index > 0 {
                output.push('\n');
            }
            output.push_str(title);
            for (timestamp, value) in rows.iter() {
                output.push_str(&format!(
                    "\n\t{:>width$}\t{}",
                    timestamp,
                    value,
                    width = timestamp_width
                ));
            }
            if rows.is_empty() {
                output.push_str("\n\t<empty>");
            }
        }
        output
    }

    pub fn get<T: Metric>(&self) -> Result<&TimeSeriesBuffer<T>, String> {
        self.store
            .get(&TypeId::of::<T>())
            .and_then(|b| b.as_any().downcast_ref())
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

/// An evaluator is a metric producer that produces new metrics from a set of
/// input dependencies every tick. The declaration of metrics it produces and consumes
/// defines its execution order in the DAG planner.
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
