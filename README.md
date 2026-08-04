# **BrainLess**

BrainLess is a lightweight, compile-time safe, real-time dataflow DAG engine written in Rust. It is engineered for low-latency metric evaluation, time-series ring-buffer management, and DuckDB time-series database (TSDB) synchronization.

Designed specifically for local-first, streaming-heavy applications like Brain-Computer Interfaces (BCI), biosensors, and high-frequency telemetry, MetricEngine offers microsecond-level execution ticks alongside effortless historical session replays.

## **Architecture at a Glance**

Hardware Sensors / APIs  
          │  
          ▼  
┌────────────────────────┐  
│ External Ingestion │  
└───────────┬────────────┘  
          │  
          ▼               Reads                 Executes DAG  
┌────────────────────────────┐  ───────  ┌───────────────────────────────┐  
│ TimeSeries Buffers    │ ────────► │ Evaluator Sequence       │  
│ & Read-Only Ledger    │ ◄───────  │ (Pure functional pass)   │  
└───────────┬────────────────┘ Commits  └───────────────────────────────┘  
          │  
          ▼ Periodic Batch Flush (20s)  
┌─────────────────────────────┐  
│ DuckDB TSDB Persistence│  
└─────────────────────────────┘

## **Quick Example**

```
use std::any::TypeId;  
use metric\_engine::{MetricEngine, FaaVolatilityMetric, RawEegMetric};

fn main() \-\> Result\<(), String\> {  
    // 1\. Declare target metrics you wish to extract  
    let targets \= vec\!\[TypeId::of::\<FaaVolatilityMetric\>()\];

    // 2\. Initialize engine (automatically builds DAG & allocates optimal buffers)  
    let mut engine \= MetricEngine::new(targets, 20\_000)?;

    // 3\. Live loop execution tick  
    loop {  
        // Ingest hardware data outside the DAG  
        engine.ingest\_external\_sample(1000, RawEegMetric { tp9: 12.0, af7: 8.5, af8: 5.1, tp10: 9.2 });

        // Run DAG evaluation pass  
        let outputs \= engine.tick(1000)?;

        // Consume calculated metrics  
        if let Some(volatility) \= outputs.get\_data::\<FaaVolatilityMetric\>() {  
            println\!("Live Volatility: {:.4}", volatility.volatility);  
        }  
    }  
}
```

## **License**

MIT License. Free for open-source and commercial use.
