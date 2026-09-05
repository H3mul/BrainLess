use super::*;

use crate::engine::MetricRegistration;
use crate::test_fixtures::{DoublingEvaluator, RecordingBackend, TestDerived, TestSource};

fn test_engine() -> MetricEngine {
    MetricEngine::builder()
        .register_evaluator(DoublingEvaluator)
        .with_metrics(HashSet::from([
            MetricRegistration::ephemeral::<TestSource>(),
            MetricRegistration::persistent::<TestDerived>(),
        ]))
        .build()
        .unwrap()
}

fn session_config(flush_interval_ms: i64) -> LiveSessionConfig {
    LiveSessionConfig {
        buffer_size_ms: 60_000,
        flush_interval_ms,
        source_metrics: HashSet::from([MetricId::of::<TestSource>()]),
        output_metrics: None,
    }
}

#[test]
fn session_buffers_plan_metrics_and_flushes_on_tick() {
    let backend = RecordingBackend::default();
    let engine = test_engine();
    let mut session = engine
        .live_session_with_storage(session_config(10_000), Box::new(backend.clone()))
        .unwrap();

    session.push_metric(1_000, TestSource { value: 2.0 });
    session.tick_at(1_000);

    // Derived metric computed and buffered, but the interval has not elapsed.
    assert!(backend.state.lock().unwrap().flushes.is_empty());
    let snapshot = session.get_metric_snapshot_at(1_000);
    assert_eq!(
        snapshot
            .get_buffer::<TestDerived>()
            .unwrap()
            .get_sample_latest()
            .unwrap()
            .data
            .value,
        4.0
    );

    // Interval elapsed on this tick: the derived metric flushes to the
    // backend while the ephemeral source does not.
    session.push_metric(12_000, TestSource { value: 3.0 });
    session.tick_at(12_000);

    let state = backend.state.lock().unwrap();
    assert_eq!(state.flushes.len(), 1);
    assert_eq!(state.flushes[0].table, "test_derived");
    assert_eq!(
        state.flushes[0].rows,
        vec![
            vec!["1000".to_string(), "4".to_string()],
            vec!["12000".to_string(), "6".to_string()],
        ]
    );
}

#[test]
fn default_live_session_uses_noop_backend() {
    let engine = test_engine();
    let mut session = engine.live_session(session_config(0)).unwrap();

    session.push_metric(1_000, TestSource { value: 1.0 });
    session.tick_at(1_000);
    assert_eq!(session.flush().unwrap(), 0);
}

#[test]
fn explicit_flush_drains_pending_samples() {
    let backend = RecordingBackend::default();
    let engine = test_engine();
    let mut session = engine
        .live_session_with_storage(session_config(60_000), Box::new(backend.clone()))
        .unwrap();

    session.push_metric(1_000, TestSource { value: 5.0 });
    session.tick_at(1_000);
    assert!(backend.state.lock().unwrap().flushes.is_empty());

    assert_eq!(session.flush().unwrap(), 1);
    assert_eq!(backend.state.lock().unwrap().flushes.len(), 1);
}
