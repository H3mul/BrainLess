use super::*;

use crate::test_fixtures::{RecordingBackend, TestDerived};
use crate::{MetricSample, NoopStorageBackend};

use std::collections::HashSet;

fn sample(timestamp_ms: i64, value: f64) -> MetricSample<TestDerived> {
    MetricSample {
        timestamp_ms,
        data: TestDerived { value },
    }
}

/// Driver with a codec registered for `TestDerived` and a matching buffer in
/// the store.
fn driver_with_store(
    backend: &RecordingBackend,
    flush_interval_ms: i64,
) -> (PersistenceDriver, BufferStore) {
    let mut driver = PersistenceDriver::new(flush_interval_ms, Box::new(backend.clone()));
    let mut store = BufferStore::new();
    store.register_buffer::<TestDerived>(60_000, &HashSet::new());
    driver.register_metric::<TestDerived>();
    (driver, store)
}

#[test]
fn flush_respects_interval_gate_and_watermarks() {
    let backend = RecordingBackend::default();
    let (mut driver, mut store) = driver_with_store(&backend, 10_000);

    store.push_sample(sample(1_000, 1.0));
    store.push_sample(sample(2_000, 2.0));

    // Interval not elapsed since the zero watermark: nothing flushed.
    assert_eq!(driver.maybe_flush(&mut store, 3_000).unwrap(), 0);
    assert!(backend.state.lock().unwrap().flushes.is_empty());

    // Explicit flush bypasses the gate and drains both samples.
    assert_eq!(driver.flush(&mut store, 3_000).unwrap(), 1);
    {
        let state = backend.state.lock().unwrap();
        assert_eq!(state.flushes.len(), 1);
        assert_eq!(state.flushes[0].table, "test_derived");
        assert_eq!(state.flushes[0].columns, vec!["value"]);
        assert_eq!(
            state.flushes[0].rows,
            vec![
                vec!["1000".to_string(), "1".to_string()],
                vec!["2000".to_string(), "2".to_string()],
            ]
        );
    }

    // No new samples: flush is a no-op.
    assert_eq!(driver.flush(&mut store, 3_000).unwrap(), 0);

    // Only samples above the watermark are flushed.
    store.push_sample(sample(3_500, 3.0));
    assert_eq!(driver.flush(&mut store, 3_500).unwrap(), 1);
    assert_eq!(
        backend.state.lock().unwrap().flushes[1].rows,
        vec![vec!["3500".to_string(), "3".to_string()]]
    );
}

#[test]
fn failed_flush_keeps_watermark_and_retries() {
    let backend = RecordingBackend::default();
    let (mut driver, mut store) = driver_with_store(&backend, 10_000);

    store.push_sample(sample(5_000, 5.0));

    backend.state.lock().unwrap().failing = true;
    assert!(driver.flush(&mut store, 10_000).is_err());
    assert!(backend.state.lock().unwrap().flushes.is_empty());

    backend.state.lock().unwrap().failing = false;
    assert_eq!(driver.flush(&mut store, 11_000).unwrap(), 1);
    assert_eq!(
        backend.state.lock().unwrap().flushes[0].rows,
        vec![vec!["5000".to_string(), "5".to_string()]]
    );
}

#[test]
fn noop_backend_skips_flush() {
    let mut driver = PersistenceDriver::new(0, Box::new(NoopStorageBackend));
    let mut store = BufferStore::new();
    store.register_buffer::<TestDerived>(60_000, &HashSet::new());
    driver.register_metric::<TestDerived>();

    store.push_sample(sample(1_000, 1.0));
    assert_eq!(driver.flush(&mut store, 1_000).unwrap(), 0);
    assert_eq!(driver.maybe_flush(&mut store, 2_000).unwrap(), 0);
}
