//! Prometheus text exposition for `GET /metrics`.
//!
//! Labels stay on a fixed set. SQL text, account ids, and database ids are not series.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use osd1_types::ResultShape;
use prometheus::core::Collector;
use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};

const BUCKETS: &[f64] = &[
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    2.5, 5.0, 10.0,
];

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Registry,
    http_requests: IntCounterVec,
    http_duration: HistogramVec,
    sql_requests: IntCounterVec,
    sql_duration: HistogramVec,
    sql_statements: IntCounter,
    rows_read: IntCounter,
    rows_written: IntCounter,
    database_size: IntGauge,
    inflight: IntGauge,
    queue_capacity: IntGauge,
    raft: std::sync::Mutex<Option<RaftGauges>>,
}

struct RaftGauges {
    role: IntGauge,
    term: IntGauge,
    commit_index: IntGauge,
    apply_index: IntGauge,
    peers: IntGauge,
}

pub struct SqlObservation {
    pub seconds: f64,
    pub statements: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub database_size_bytes: u64,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let http_requests = counter_vec(
            "osd1_http_requests_total",
            "HTTP responses by route and status",
            &["route", "status"],
        );
        let http_duration = histogram_vec(
            "osd1_http_request_duration_seconds",
            "HTTP request time, including queue wait and JSON encoding",
            &["route"],
        );
        let sql_requests = counter_vec(
            "osd1_sql_requests_total",
            "SQL requests that reached the queue or the worker",
            &["result", "route"],
        );
        let sql_duration = histogram_vec(
            "osd1_sql_duration_seconds",
            "Time spent on the SQL thread for one request, excluding queue wait",
            &["route"],
        );
        let sql_statements = counter(
            "osd1_sql_statements_total",
            "SQL statements that finished inside a request",
        );
        let rows_read = counter(
            "osd1_rows_read_total",
            "Rows returned by finished statements",
        );
        let rows_written = counter(
            "osd1_rows_written_total",
            "Rows written by finished statements",
        );
        let database_size = gauge(
            "osd1_database_size_bytes",
            "Main database file length after the latest finished request",
        );
        let inflight = gauge(
            "osd1_sql_inflight",
            "Accepted SQL requests that have not finished, including the one running",
        );
        let queue_capacity = gauge(
            "osd1_sql_queue_capacity",
            "Requests that may wait. The running request is outside this bound",
        );
        let build = IntCounterVec::new(
            Opts::new(
                "osd1_build_info",
                "Build information. The value is always 1",
            ),
            &["version"],
        )
        .expect("build info");
        build.with_label_values(&[env!("CARGO_PKG_VERSION")]).inc();
        let started = Gauge::with_opts(Opts::new(
            "osd1_process_start_time_seconds",
            "Unix time when this process started exporting metrics",
        ))
        .expect("start time");
        started.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs_f64())
                .unwrap_or(0.0),
        );

        for collector in [
            Box::new(http_requests.clone()) as Box<dyn Collector>,
            Box::new(http_duration.clone()),
            Box::new(sql_requests.clone()),
            Box::new(sql_duration.clone()),
            Box::new(sql_statements.clone()),
            Box::new(rows_read.clone()),
            Box::new(rows_written.clone()),
            Box::new(database_size.clone()),
            Box::new(inflight.clone()),
            Box::new(queue_capacity.clone()),
            Box::new(build),
            Box::new(started),
        ] {
            registry.register(collector).expect("metric name is unique");
        }

        Self {
            inner: Arc::new(Inner {
                registry,
                http_requests,
                http_duration,
                sql_requests,
                sql_duration,
                sql_statements,
                rows_read,
                rows_written,
                database_size,
                inflight,
                queue_capacity,
                raft: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Register the raft series. Simple mode never calls this, so those names
    /// stay off `/metrics`. Role is one gauge: 0 follower, 1 leader, 2 candidate, 3 learner.
    pub fn enable_raft(&self) {
        let mut slot = self.inner.raft.lock().expect("raft metrics");
        if slot.is_some() {
            return;
        }
        let role = gauge(
            "osd1_raft_role",
            "Raft role: 0 follower, 1 leader, 2 candidate, 3 learner",
        );
        let term = gauge("osd1_raft_term", "Current raft term");
        let commit_index = gauge(
            "osd1_raft_commit_index",
            "Quorum-committed log index on this node",
        );
        let apply_index = gauge(
            "osd1_raft_apply_index",
            "Log index applied to this node's database",
        );
        let peers = gauge(
            "osd1_raft_peers",
            "Nodes in the current membership, including this one",
        );
        for collector in [
            Box::new(role.clone()) as Box<dyn Collector>,
            Box::new(term.clone()),
            Box::new(commit_index.clone()),
            Box::new(apply_index.clone()),
            Box::new(peers.clone()),
        ] {
            self.inner
                .registry
                .register(collector)
                .expect("raft metric name is unique");
        }
        *slot = Some(RaftGauges {
            role,
            term,
            commit_index,
            apply_index,
            peers,
        });
    }

    pub fn set_raft(&self, role: i64, term: u64, commit: u64, apply: u64, peers: u64) {
        let slot = self.inner.raft.lock().expect("raft metrics");
        let Some(gauges) = slot.as_ref() else {
            return;
        };
        gauges.role.set(role);
        gauges.term.set(term as i64);
        gauges.commit_index.set(commit as i64);
        gauges.apply_index.set(apply as i64);
        gauges.peers.set(peers as i64);
    }

    pub fn observe_http(&self, route: &'static str, status: u16, seconds: f64) {
        let status = status_label(status);
        self.inner
            .http_requests
            .with_label_values(&[route, status])
            .inc();
        self.inner
            .http_duration
            .with_label_values(&[route])
            .observe(seconds);
    }

    pub fn observe_sql(
        &self,
        result: &'static str,
        shape: ResultShape,
        observation: Option<SqlObservation>,
    ) {
        let route = route_for_shape(shape);
        self.inner
            .sql_requests
            .with_label_values(&[result, route])
            .inc();
        let Some(observation) = observation else {
            return;
        };
        self.inner
            .sql_duration
            .with_label_values(&[route])
            .observe(observation.seconds);
        self.inner.sql_statements.inc_by(observation.statements);
        self.inner.rows_read.inc_by(observation.rows_read);
        self.inner.rows_written.inc_by(observation.rows_written);
        self.inner
            .database_size
            .set(observation.database_size_bytes as i64);
    }

    pub fn set_queue(&self, inflight: usize, capacity: usize) {
        self.inner.inflight.set(inflight as i64);
        self.inner.queue_capacity.set(capacity as i64);
    }

    pub fn encode(&self) -> Vec<u8> {
        let families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut buffer)
            .expect("metrics are valid text");
        buffer
    }
}

pub fn route_label(path: &str) -> &'static str {
    if path == "/health" {
        "health"
    } else if path == "/metrics" {
        "metrics"
    } else if path == "/" || path == "/explorer" {
        "explorer"
    } else if path.ends_with("/query") {
        "query"
    } else if path.ends_with("/raw") {
        "raw"
    } else {
        "other"
    }
}

pub fn route_for_shape(shape: ResultShape) -> &'static str {
    match shape {
        ResultShape::Objects => "query",
        ResultShape::Raw => "raw",
    }
}

fn status_label(status: u16) -> &'static str {
    match status {
        200 => "200",
        400 => "400",
        401 => "401",
        413 => "413",
        429 => "429",
        500 => "500",
        504 => "504",
        _ => "other",
    }
}

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::with_opts(Opts::new(name, help)).expect(name)
}

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect(name)
}

fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::with_opts(Opts::new(name, help)).expect(name)
}

fn histogram_vec(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(BUCKETS.to_vec()),
        labels,
    )
    .expect(name)
}
