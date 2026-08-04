use bci_metrics::{FaaVolatilityMetric, RawEegMetric, build_engine};

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

    let ledger = engine.tick(1_000)?;

    if let Some(sample) = ledger.get::<FaaVolatilityMetric>()?.latest() {
        println!("Current volatility: {}", sample.data.volatility);
    }

    Ok(())
}
