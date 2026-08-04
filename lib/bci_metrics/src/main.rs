use bci_metrics::{build_engine, FaaVolatilityMetric, RawEegMetric};

fn main() -> Result<(), String> {
    let mut engine = build_engine()?;

    engine.feed_external(
        1_000,
        RawEegMetric {
            tp9: 12.0,
            af7: 8.5,
            af8: 5.1,
            tp10: 9.2,
        },
    );

    let outputs = engine.tick(1_000)?;

    if let Some(metric) = outputs.get_latest::<FaaVolatilityMetric>() {
        println!("Current volatility: {}", metric.volatility);
    }

    Ok(())
}
