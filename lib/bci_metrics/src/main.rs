use bci_metrics::{RawEegMetric, build_engine};

fn main() -> Result<(), String> {
    let mut engine = build_engine()?;
    engine.feed(
        1_000,
        RawEegMetric {
            tp9: 12.0,
            af7: 8.5,
            af8: 5.1,
            tp10: 9.2,
        },
    );

    engine.tick(1_000)?;

    engine.feed(
        1_001,
        RawEegMetric {
            tp9: 12.0,
            af7: 8.5,
            af8: 5.1,
            tp10: 5.2,
        },
    );

    engine.tick(1_001)?;
    let ledger = engine.tick(1_002)?;

    println!("Metric ledger:\n{}", ledger.pretty_print());
    Ok(())
}
