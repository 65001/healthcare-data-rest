//! In-memory background job tracker.
//!
//! Architecture.md specifies `jobs.rs` but not whether job state should
//! survive a restart. Decision made here (undocumented in the spec, so
//! flagging it): jobs are tracked **in-memory only** via a `RwLock`-guarded
//! map, not persisted to SQLite. A restart loses in-flight/historical job
//! status, which is acceptable for on-demand pipeline triggers (the
//! underlying data — hospitals, discoveries — is durable; only the job
//! bookkeeping is not) but worth revisiting if jobs need to survive a
//! deploy or be queryable after a crash.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JobProgress {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
}

impl Default for JobProgress {
    fn default() -> Self {
        Self { total: 0, completed: 0, failed: 0 }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Job {
    pub id: String,
    pub stage: String,
    pub status: JobStatus,
    pub progress: JobProgress,
    #[schema(value_type = String, format = "date-time")]
    pub started_at: DateTime<Utc>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Clone, Default)]
pub struct JobTracker {
    jobs: Arc<RwLock<HashMap<String, Job>>>,
}

impl JobTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, stage: &str) -> Job {
        let job = Job {
            id: Uuid::new_v4().to_string(),
            stage: stage.to_string(),
            status: JobStatus::Pending,
            progress: JobProgress::default(),
            started_at: Utc::now(),
            finished_at: None,
            error: None,
        };
        self.jobs.write().unwrap().insert(job.id.clone(), job.clone());
        job
    }

    pub fn get(&self, id: &str) -> Option<Job> {
        self.jobs.read().unwrap().get(id).cloned()
    }

    pub fn update<F: FnOnce(&mut Job)>(&self, id: &str, f: F) {
        if let Some(job) = self.jobs.write().unwrap().get_mut(id) {
            f(job);
        }
    }
}
