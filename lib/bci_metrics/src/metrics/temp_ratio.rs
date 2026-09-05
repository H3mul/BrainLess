use metric_engine::Metric;

/// Experimental in-memory ratio derived from the FAA metric.
#[derive(Clone, Debug)]
pub struct TempRatioMetric {
    pub ratio: f32,
}
impl Metric for TempRatioMetric {
    fn id() -> &'static str {
        "temp_ratio"
    }
}
