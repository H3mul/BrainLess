use metric_engine::Metric;

#[derive(Clone, Debug)]
pub struct TempRatioMetric {
    pub ratio: f32,
}
impl Metric for TempRatioMetric {}
