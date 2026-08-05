use bci_metrics::{RawEegMetric, build_engine};
use metric_engine::LiveSessionConfig;

fn main() -> Result<(), String> {
    let engine = build_engine()?;
    let mut live = engine.live_session(
        LiveSessionConfig::default(),
        bci_metrics::DuckDbBackend::new("./eeg_sessions.db"),
    )?;
    live.push_live_metric(RawEegMetric {
        tp9: 12.0,
        af7: 8.5,
        af8: 5.1,
        tp10: 9.2,
    });
    let ledger = live.tick()?;
    println!("Metric ledger:\n{}", ledger.pretty_print());
    Ok(())
}
