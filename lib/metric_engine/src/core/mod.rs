use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet, VecDeque};

pub trait Metric: Send + Sync + Clone + 'static {}

pub trait ExternalMetric: Metric {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SampleRate {
    Best,
    Hz(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Age {
    Latest,
    SecondsAgo(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AgeRange {
    Single(Age),
    Window {
        start: Age,
        end: Age,
        rate: SampleRate,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricDependency {
    pub type_id: TypeId,
    pub range: AgeRange,
}

impl MetricDependency {
    pub fn latest<T: Metric>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            range: AgeRange::Single(Age::Latest),
        }
    }

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

#[derive(Debug, Clone)]
pub struct MetricSample<T: Metric> {
    pub timestamp_ms: i64,
    pub data: T,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesBuffer<T: Metric> {
    pub samples: VecDeque<MetricSample<T>>,
    pub capacity: usize,
}

impl<T: Metric> TimeSeriesBuffer<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
        }
    }

    pub fn push_latest(&mut self, timestamp_ms: i64, data: T) {
        self.push_sample(MetricSample { timestamp_ms, data });
    }

    pub fn push_sample(&mut self, sample: MetricSample<T>) {
        if self.samples.len() == self.capacity {
            self.samples.pop_front();
        }

        self.samples.push_back(sample);
    }

    pub fn merge_historic(&mut self, historic: Vec<MetricSample<T>>) {
        let mut all: Vec<_> = self.samples.drain(..).chain(historic).collect();
        all.sort_by_key(|s| s.timestamp_ms);
        all.dedup_by_key(|s| s.timestamp_ms);
        let start = all.len().saturating_sub(self.capacity);
        self.samples = all[start..].iter().cloned().collect();
    }

    pub fn latest(&self) -> Option<&MetricSample<T>> {
        self.samples.back()
    }

    pub fn as_slice(&self) -> &VecDeque<MetricSample<T>> {
        &self.samples
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

#[derive(Default)]
pub struct TickLedger {
    pub timestamp_ms: i64,
    store: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl TickLedger {
    pub fn new(timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms,
            store: HashMap::new(),
        }
    }

    pub fn insert_series<T: Metric>(&mut self, series: TimeSeriesBuffer<T>) {
        self.store.insert(TypeId::of::<T>(), Box::new(series));
    }

    pub fn insert_erased(&mut self, id: TypeId, series: Box<dyn Any + Send + Sync>) {
        self.store.insert(id, series);
    }

    pub fn get<T: Metric>(&self) -> Result<&TimeSeriesBuffer<T>, String> {
        self.store
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
            .ok_or_else(|| format!("Metric type {:?} missing from ledger", TypeId::of::<T>()))
    }
}

pub trait MetricGroup: Send + Sync + 'static {
    fn type_ids() -> HashSet<TypeId>;

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

pub trait MetricEvaluator: Send + Sync {
    type Output: MetricGroup;

    fn id(&self) -> &'static str;

    fn dependencies(&self) -> HashSet<MetricDependency>;

    fn evaluate(&self, ledger: &TickLedger) -> Result<Self::Output, String>;
}

pub trait ErasedEvaluator: Send + Sync {
    fn id(&self) -> &'static str;

    fn produces(&self) -> HashSet<TypeId>;

    fn dependencies(&self) -> HashSet<MetricDependency>;

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
