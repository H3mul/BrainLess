use super::*;

use crate::engine::MetricRegistration;
use crate::test_fixtures::{
    DoublingEvaluator, RecordingBackend, TestAggMetric, TestDerived, TestSource,
    WindowCountEvaluator,
};

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
    session.tick(1_000);

    // Derived metric computed and buffered, but the interval has not elapsed.
    assert!(backend.state.lock().unwrap().flushes.is_empty());
    let snapshot = session.time_series();
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

    // The tick at 12_000 trips the flush interval: the evaluator fires
    // first, then maintenance flushes the pending derived samples (the
    // 1_000 tick never met the interval gate).
    session.push_metric(12_000, TestSource { value: 3.0 });
    session.tick(12_000);

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
fn tick_evicts_outside_retention_after_flushing() {
    let backend = RecordingBackend::default();
    let engine = test_engine();
    // Retention floor is twice the flush interval: 2 x 10s = 20s.
    let config = LiveSessionConfig {
        buffer_size_ms: 0,
        flush_interval_ms: 10_000,
        source_metrics: HashSet::from([MetricId::of::<TestSource>()]),
        output_metrics: None,
    };
    let mut session = engine
        .live_session_with_storage(config, Box::new(backend.clone()))
        .unwrap();

    session.push_metric(1_000, TestSource { value: 1.0 });
    session.tick(1_000); // derived@1000 = 2.0 committed

    // The tick trips both the flush interval and retention: the 1_000
    // samples are flushed, then evicted (25_000 - 20_000 = 5_000).
    session.push_metric(25_000, TestSource { value: 2.0 });
    session.tick(25_000);

    {
        let state = backend.state.lock().unwrap();
        // Flushed before eviction, so nothing is lost. The derived sample
        // committed by this tick (25_000 = 4.0) flushes alongside the older
        // one.
        assert_eq!(state.flushes.len(), 1);
        assert_eq!(
            state.flushes[0].rows,
            vec![
                vec!["1000".to_string(), "2".to_string()],
                vec!["25000".to_string(), "4".to_string()],
            ]
        );
    }

    let snapshot = session.time_series();
    let source = snapshot.get_buffer::<TestSource>().unwrap();
    assert_eq!(source.len(), 1);
    assert_eq!(source.get_sample_latest().unwrap().timestamp_ms, 25_000);
    // Derived sample at 1_000 was flushed and then evicted; the fresh one at
    // 25_000 stays inside the retention window.
    let derived = snapshot.get_buffer::<TestDerived>().unwrap();
    assert_eq!(derived.len(), 1);
    assert_eq!(derived.get_sample_latest().unwrap().timestamp_ms, 25_000);
}

#[test]
fn retention_honors_evaluator_demand_over_flush_floor() {
    let engine = MetricEngine::builder()
        .register_evaluator(WindowCountEvaluator)
        .with_metrics(HashSet::from([
            MetricRegistration::ephemeral::<TestSource>(),
            MetricRegistration::ephemeral::<TestAggMetric>(),
        ]))
        .build()
        .unwrap();
    let config = LiveSessionConfig {
        buffer_size_ms: 0,
        flush_interval_ms: 10_000,
        source_metrics: HashSet::from([MetricId::of::<TestSource>()]),
        output_metrics: None,
    };
    let mut session = engine.live_session(config).unwrap();

    // 25s old at the next tick: outside the 20s flush floor but inside the
    // evaluator's 60s window demand.
    session.push_metric(45_000, TestSource { value: 1.0 });
    session.push_metric(70_000, TestSource { value: 2.0 });
    session.tick(70_000);

    let snapshot = session.time_series();
    let source = snapshot.get_buffer::<TestSource>().unwrap();
    assert_eq!(
        source
            .get_samples()
            .iter()
            .map(|sample| sample.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![45_000, 70_000]
    );
    assert_eq!(
        snapshot
            .get_buffer::<TestAggMetric>()
            .unwrap()
            .get_sample_latest()
            .unwrap()
            .data
            .count,
        2.0
    );
}

#[test]
fn tick_trips_flush_for_directly_fed_persistent_metric() {
    // A persistent metric fed directly as a source: the tick must trip the
    // flush interval even though the source produced the samples itself.
    let backend = RecordingBackend::default();
    let engine = MetricEngine::builder()
        .with_metrics(HashSet::from([
            MetricRegistration::persistent::<TestDerived>(),
        ]))
        .build()
        .unwrap();
    let config = LiveSessionConfig {
        buffer_size_ms: 60_000,
        flush_interval_ms: 10_000,
        source_metrics: HashSet::from([MetricId::of::<TestDerived>()]),
        output_metrics: None,
    };
    let mut session = engine
        .live_session_with_storage(config, Box::new(backend.clone()))
        .unwrap();

    session.push_metric(9_000, TestDerived { value: 1.0 });
    session.push_metric(12_000, TestDerived { value: 2.0 });
    assert!(backend.state.lock().unwrap().flushes.is_empty());

    session.tick(12_000);
    let state = backend.state.lock().unwrap();
    assert_eq!(state.flushes.len(), 1);
    assert_eq!(
        state.flushes[0].rows,
        vec![
            vec!["9000".to_string(), "1".to_string()],
            vec!["12000".to_string(), "2".to_string()],
        ]
    );
}

#[test]
fn tick_result_empty_before_first_tick() {
    let engine = test_engine();
    let mut session = engine.live_session(session_config(10_000)).unwrap();

    assert!(session.last_tick().is_empty());

    // Pushes do not publish; only ticks do.
    session.push_metric(1_000, TestSource { value: 1.0 });
    assert!(session.last_tick().is_empty());
}

#[test]
fn tick_returns_tick_result_and_published_views_stay_consistent() {
    let engine = test_engine();
    let mut session = engine.live_session(session_config(10_000)).unwrap();

    session.push_metric(1_000, TestSource { value: 2.0 });
    let first = session.tick(1_000);
    assert_eq!(
        first
            .get::<TestSource>()
            .map(|(timestamp, value)| (timestamp, value.value)),
        Some((1_000, 2.0))
    );
    assert_eq!(
        first
            .get::<TestDerived>()
            .map(|(timestamp, value)| (timestamp, value.value)),
        Some((1_000, 4.0))
    );
    assert_eq!(session.last_tick().len(), 2);

    // The next tick republishes; the previously returned pointer is
    // immutable and still reflects its own tick.
    session.push_metric(2_000, TestSource { value: 3.0 });
    let second = session.tick(2_000);
    assert_eq!(
        second
            .get::<TestDerived>()
            .map(|(timestamp, value)| (timestamp, value.value)),
        Some((2_000, 6.0))
    );
    assert_eq!(
        first
            .get::<TestDerived>()
            .map(|(timestamp, value)| (timestamp, value.value)),
        Some((1_000, 4.0))
    );
    assert_eq!(
        session
            .last_tick()
            .get::<TestSource>()
            .map(|(timestamp, value)| (timestamp, value.value)),
        Some((2_000, 3.0))
    );

    // The on-demand time-series view introspects the full retained history,
    // while the tick results only ever carry one sample per metric.
    let series = session.time_series();
    assert_eq!(series.get_buffer::<TestSource>().unwrap().len(), 2);
    assert_eq!(series.get_buffer::<TestDerived>().unwrap().len(), 2);
    assert!(first.get::<TestSource>().is_some());
}

#[test]
fn default_live_session_uses_noop_backend() {
    let engine = test_engine();
    let mut session = engine.live_session(session_config(0)).unwrap();

    session.push_metric(1_000, TestSource { value: 1.0 });
    session.tick(1_000);
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
    session.tick(1_000);
    assert!(backend.state.lock().unwrap().flushes.is_empty());

    assert_eq!(session.flush().unwrap(), 1);
    assert_eq!(backend.state.lock().unwrap().flushes.len(), 1);
}
