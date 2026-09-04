use std::{any::TypeId, collections::HashSet};

use bci_metrics::{RawEegMetric, build_eeg_engine};
use metric_engine::LiveSessionConfig;

fn main() -> Result<(), String> {
    let engine = build_eeg_engine()?;
    let mut live = engine.live_session(LiveSessionConfig {
        source_metrics: HashSet::from([TypeId::of::<RawEegMetric>()]),
        buffer_size_ms: 30_000,
        flush_interval_ms: 10_000,
        output_metrics: None,
    })?;

    live.feed_live_metric(RawEegMetric {
        tp9: 12.0,
        af7: 8.5,
        af8: 5.1,
        tp10: 9.2,
    })?;

    let ledger = live.feed_live_metric(RawEegMetric {
        tp9: 12.0,
        af7: 10.5,
        af8: 5.1,
        tp10: 11.2,
    })?;

    println!("Metric ledger:\n{}", ledger.pretty_print());
    Ok(())
}
