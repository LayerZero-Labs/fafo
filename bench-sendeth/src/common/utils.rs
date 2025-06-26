use log::{debug, info};
use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;

pub fn reset_dir(ref_dir: &str, run_dir: &str) {
    // Use rsync to reset directory to the reference, fall back to cp if rsync is unavailable/unsupported
    info!("Syncing directory from {} to {}...", ref_dir, run_dir);

    // Try rsync first (without --info=progress2 which is unsupported on macOS)
    let rsync_status = Command::new("rsync")
        .arg("-a")
        .arg(format!("{}/", ref_dir))
        .arg(format!("{}/", run_dir))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status();

    let use_cp_fallback = match rsync_status {
        Ok(status) => {
            debug!("rsync exited with: {}", status);
            !status.success()
        }
        Err(err) => {
            debug!("failed to execute rsync: {}", err);
            true
        }
    };

    if use_cp_fallback {
        info!("Falling back to cp -R to copy directory contents...");
        let cp_status = Command::new("cp")
            .arg("-R")
            .arg("-f")
            .arg(format!("{}/", ref_dir))
            .arg(format!("{}/", run_dir))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("Failed to execute cp command");
        debug!("cp exited with: {}", cp_status);
    }

    // Brief pause to ensure filesystem settles before subsequent operations
    thread::sleep(std::time::Duration::from_millis(100));
}

pub fn init_logging(max_level: &str) {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or(max_level));
}

#[derive(Serialize)]
pub struct HistogramStats {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    /// Mapping from percentile label (e.g., "p90", "p999") to value.
    pub percentiles: BTreeMap<String, f64>,
}

impl HistogramStats {
    /// Compute histogram statistics and selected percentiles from a slice of raw values.
    pub fn compute(samples: &[ordered_float::OrderedFloat<f64>]) -> Self {
        let len = samples.len();
        let mut values: Vec<f64> = samples.iter().map(|v| v.into_inner()).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Early return for empty histograms.
        if len == 0 {
            return Self {
                count: 0,
                min: 0.0,
                max: 0.0,
                mean: 0.0,
                percentiles: BTreeMap::new(),
            };
        }

        let sum: f64 = values.iter().sum();
        let mean = sum / len as f64;

        let percentile = |pct: f64| -> f64 {
            let idx = ((pct / 100.0) * (len as f64 - 1.0)).round() as usize;
            values[idx]
        };

        // Define the percentiles we want to record.
        const PCTS: &[(f64, &str)] = &[
            (10.0, "p10"),
            (20.0, "p20"),
            (25.0, "p25"),
            (30.0, "p30"),
            (40.0, "p40"),
            (50.0, "p50"),
            (60.0, "p60"),
            (70.0, "p70"),
            (75.0, "p75"),
            (80.0, "p80"),
            (90.0, "p90"),
            (95.0, "p95"),
            (99.0, "p99"),
            (99.9, "p999"),
            (99.99, "p9999"),
        ];

        let mut pct_map = BTreeMap::new();
        for (pct, label) in PCTS {
            pct_map.insert((*label).to_string(), percentile(*pct));
        }

        Self {
            count: len,
            min: values[0],
            max: values[len - 1],
            mean,
            percentiles: pct_map,
        }
    }
}

#[derive(Serialize)]
pub struct MetricDump {
    pub name: String,
    pub unit: Option<String>,
    pub description: Option<String>,
    /// For counters and gauges, holds the raw numeric value. `None` for histograms.
    pub value: Option<String>,
    /// Detailed histogram percentiles, if this metric is a histogram.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram: Option<HistogramStats>,
}

pub struct MetricsRecorder {
    snapshotter: Snapshotter,
    /// Cached latest numeric values for quick lookup between snapshots
    last_values: RefCell<HashMap<String, f64>>, // interior mutability, single-threaded
}

impl Default for MetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder {
    pub fn new() -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().unwrap();
        Self {
            snapshotter,
            last_values: RefCell::new(HashMap::new()),
        }
    }

    pub fn snapshot_metrics(&self) -> HashMap<String, MetricDump> {
        let snapshot = self.snapshotter.snapshot();

        snapshot
            .into_vec()
            .into_iter()
            .map(|(ckey, unit, description, value)| {
                let name = ckey.key().name().to_owned();
                (name.clone(), {
                    let histogram = if let metrics_util::debugging::DebugValue::Histogram(
                        ref samples,
                    ) = value
                    {
                        Some(HistogramStats::compute(samples))
                    } else {
                        None
                    };
                    MetricDump {
                        name,
                        unit: unit.map(|u| u.as_str().to_owned()),
                        description: description.map(|d| format!("{}", d)),
                        value: match &value {
                            metrics_util::debugging::DebugValue::Counter(v) => Some(v.to_string()),
                            metrics_util::debugging::DebugValue::Gauge(v) => {
                                Some(v.into_inner().to_string())
                            }
                            metrics_util::debugging::DebugValue::Histogram(_) => None,
                        },
                        histogram,
                    }
                })
            })
            .collect()
    }

    pub fn dump(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data = self.snapshot_metrics();

        let mut file = File::create(filename)?;
        let json_string = serde_json::to_string_pretty(&data)?;
        file.write_all(json_string.as_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Take a fresh snapshot and return a HashMap from metric name to its numeric value.
    /// For counters the integer value is converted to `f64`; for gauges the `f64` is used as-is.
    /// Histogram values do not have an obvious single numeric representation, so they are
    /// currently skipped.
    pub fn snapshot_values(&self) -> HashMap<String, f64> {
        use metrics_util::debugging::DebugValue;
        let snapshot = self.snapshotter.snapshot();
        let map: HashMap<String, f64> = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(ckey, _unit, _descr, value)| {
                let name = ckey.key().name().to_owned();
                match value {
                    DebugValue::Counter(v) => Some((name, v as f64)),
                    DebugValue::Gauge(v) => Some((name, v.into_inner())),
                    DebugValue::Histogram(samples) => {
                        // Re-insert samples so they aren't lost for subsequent dumps.
                        for s in &samples {
                            metrics::histogram!(name.clone()).record(s.into_inner());
                        }
                        None
                    }
                }
            })
            .collect();

        // Update internal cache
        *self.last_values.borrow_mut() = map.clone();

        map
    }

    /// Return the latest numeric value for the given metric name, if available.
    /// This is a convenience wrapper around `snapshot_values()`.
    pub fn get_value(&self, name: &str) -> Option<f64> {
        if let Some(v) = self.last_values.borrow().get(name) {
            return Some(*v);
        }
        // Otherwise, refresh snapshot and retry (will update cache)
        let _ = self.snapshot_values();
        self.last_values.borrow().get(name).copied()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use metrics::{counter, gauge, histogram};
//     use std::fs;
//     use std::io::Read;

//     #[test]
//     fn test_metrics_recorder_dump() {
//         // Create a new recorder and install it
//         let recorder = MetricsRecorder::new();

//         // Record some metrics
//         counter!("test.requests.total").increment(5);
//         gauge!("test.memory.usage_bytes").set(1024.0);
//         histogram!("test.response.duration_seconds").record(0.5);

//         // Dump to a file
//         let test_file = "test_metrics.json";
//         recorder.dump(test_file).unwrap();

//         // Verify the file exists and contains JSON data
//         assert!(std::path::Path::new(test_file).exists());

//         let mut file_content = String::new();
//         File::open(test_file)
//             .unwrap()
//             .read_to_string(&mut file_content)
//             .unwrap();

//         // Parse the JSON to verify structure and metric names
//         let data: HashMap<String, MetricDump> = serde_json::from_str(&file_content).unwrap();

//         // Check that our test metrics are present
//         assert!(data.contains_key("test.requests.total"));
//         assert!(data.contains_key("test.memory.usage_bytes"));
//         assert!(data.contains_key("test.response.duration_seconds"));

//         // Clean up
//         fs::remove_file(test_file).unwrap();
//     }
// }
