use std::collections::HashSet;

use bci_metrics::{FaaMetric, RawEegMetric, build_eeg_engine};
use metric_engine::{LiveSessionConfig, MetricId};

fn main() -> Result<(), String> {
    let engine = build_eeg_engine()?;
    let mut live = engine.live_session(LiveSessionConfig {
        source_metrics: HashSet::from([MetricId::of::<RawEegMetric>()]),
        buffer_size_ms: 30_000,
        flush_interval_ms: 10_000,
        // Only the metrics with active evaluators; the volatility/ratio
        // evaluators are still commented out upstream.
        output_metrics: Some(HashSet::from([MetricId::of::<FaaMetric>()])),
    })?;

    live.push_metric(
        10000,
        RawEegMetric {
            tp9: 12.0,
            af7: 8.5,
            af8: 5.1,
            tp10: 9.2,
        },
    );
    let first_tick = live.tick(10000);
    println!("First tick result:\n{first_tick}");

    live.push_metric(
        11000,
        RawEegMetric {
            tp9: 12.0,
            af7: 10.5,
            af8: 5.1,
            tp10: 11.2,
        },
    );
    let second_tick = live.tick(11000);
    println!("Second tick result:\n{second_tick}");

    // The handle returned by tick() (or fetched at any time via last_tick)
    // is a cheap tick-consistent read view; an earlier tick's pointer is
    // immutable and still readable.
    assert_eq!(first_tick.len(), 2);

    let (eeg_ts, eeg) = second_tick.get::<RawEegMetric>().expect("raw eeg present");
    let (faa_ts, faa) = second_tick.get::<FaaMetric>().expect("faa present");
    println!("Latest EEG @ {eeg_ts}: {eeg:?}");
    println!("Latest FAA @ {faa_ts}: {faa:?}");

    // The on-demand time-series view introspects the full buffer history.
    println!("Full time series:\n{}", live.time_series());

    Ok(())
}
