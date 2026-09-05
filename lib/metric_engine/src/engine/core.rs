use std::any::TypeId;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::BufferStore;
use crate::engine::buffer_store::ReadOnlyBufferStore;

/// Base trait implemented by every value that flows through the metric engine.
/// Structs implementing this trait are expected to be both the metric type identifier
/// and its data container.
///
/// Metrics must be cloneable because the engine keeps bounded historical copies
/// in its time-series buffers and passes read-only snapshots to evaluators.
pub trait Metric: Send + Sync + Clone + Debug + 'static {
    /// Stable identifier used in diagnostics and tracing.
    fn id() -> &'static str;

    /// Target sampling cadence for this metric. This is the ground truth for
    /// every interaction with the type: storage extraction rates and buffer
    /// introspection (whether a buffer holds a valid sample for a timestamp).
    /// Defaults to 256 Hz.
    fn sample_rate() -> SampleRate {
        SampleRate::Hz(256)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricId(TypeId);

impl MetricId {
    #[inline]
    pub fn of<T: Metric>() -> Self {
        Self(TypeId::of::<T>())
    }
}

impl SampleRate {
    /// Half-width of the timestamp window in which a sample counts as present
    /// for a target timestamp under this rate. `Best` implies no assumed
    /// cadence, so only an exact timestamp match counts.
    pub fn tolerance_ms(&self) -> i64 {
        match self {
            SampleRate::Best => 0,
            SampleRate::Hz(hz) => (1000 / *hz as i64).max(1),
        }
    }
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
    pub metric_id: MetricId,
    pub request: SampleRequest,
}

impl MetricDependency {
    /// Requests the newest available sample for `T`.
    pub fn latest<T: Metric>() -> Self {
        Self {
            metric_id: MetricId::of::<T>(),
            request: SampleRequest::Single(Age::Latest),
        }
    }

    /// Requests a bounded historical window for `T`, expressed in seconds ago.
    pub fn window<T: Metric>(start_secs: usize, end_secs: usize, rate: SampleRate) -> Self {
        Self {
            metric_id: MetricId::of::<T>(),
            request: SampleRequest::Window {
                start: Age::SecondsAgo(start_secs),
                end: Age::SecondsAgo(end_secs),
                rate,
            },
        }
    }
}

fn max_sample_rate(current: SampleRate, requested: SampleRate) -> SampleRate {
    match (current, requested) {
        (SampleRate::Best, _) | (_, SampleRate::Best) => SampleRate::Best,
        (SampleRate::Hz(current), SampleRate::Hz(requested)) => {
            SampleRate::Hz(current.max(requested))
        }
    }
}

/// Longest history requested for a metric type across all dependencies, in
/// milliseconds. Zero when no dependency asks for history.
pub fn requested_history_ms(demand: &HashSet<MetricDependency>, metric_id: MetricId) -> i64 {
    demand
        .iter()
        .filter(|dependency| dependency.metric_id == metric_id)
        .map(|dependency| match &dependency.request {
            SampleRequest::Single(Age::SecondsAgo(seconds)) => *seconds as i64 * 1_000,
            SampleRequest::Window {
                start: Age::SecondsAgo(seconds),
                ..
            } => *seconds as i64 * 1_000,
            _ => 0,
        })
        .max()
        .unwrap_or(0)
}

/// Highest sample rate requested for a metric type across all dependencies.
pub fn requested_sample_rate(
    demand: &HashSet<MetricDependency>,
    metric_id: MetricId,
) -> SampleRate {
    demand
        .iter()
        .filter(|dependency| dependency.metric_id == metric_id)
        .filter_map(|dependency| match &dependency.request {
            SampleRequest::Window { rate, .. } => Some(rate.clone()),
            _ => None,
        })
        .fold(SampleRate::Hz(256), max_sample_rate)
}

/// A metric value paired with its source timestamp.
#[derive(Debug, Clone)]
pub struct MetricSample<T: Metric> {
    pub timestamp_ms: i64,
    pub data: T,
}

/// Output shape supported by an evaluator.
///
/// Implementations commit one or more typed metric values to storage after
/// evaluation completes.
pub trait MetricGroup: Send + Sync + 'static {
    /// Returns the concrete metric types contained in this output.
    fn type_ids() -> HashSet<MetricId>;

    fn commit_to_store(self, timestamp_ms: i64, storage: &mut BufferStore);
}

impl<T: Metric> MetricGroup for T {
    fn type_ids() -> HashSet<MetricId> {
        HashSet::from([MetricId::of::<T>()])
    }

    fn commit_to_store(self, timestamp_ms: i64, store: &mut BufferStore) {
        store.push_sample(MetricSample {
            timestamp_ms,
            data: self,
        });
    }
}

impl<A: Metric, B: Metric> MetricGroup for (A, B) {
    fn type_ids() -> HashSet<MetricId> {
        HashSet::from([MetricId::of::<A>(), MetricId::of::<B>()])
    }

    fn commit_to_store(self, timestamp_ms: i64, store: &mut BufferStore) {
        store.push_sample(MetricSample {
            timestamp_ms,
            data: self.0,
        });

        store.push_sample(MetricSample {
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
    fn id() -> &'static str;

    /// Dependencies required before evaluation can run.
    fn dependencies(&self) -> HashSet<MetricDependency>;

    /// Evaluates against the read-only tick ledger.
    fn evaluate(
        &self,
        timestamp_ts: i64,
        store: &ReadOnlyBufferStore,
    ) -> Result<Self::Output, String>;
}
