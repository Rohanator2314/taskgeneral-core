//! Postgres storage backend for TaskGeneral.

use crate::error::{Result, TaskError};
use crate::models::{SortField, SyncResult, TaskFilter, TaskInfo, TaskUpdate, WorkingSetItem};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio_postgres::Client;
use uuid::Uuid;

use taskchampion::{server::ServerConfig, Operations, Replica, Status, Tag, Uuid as TcUuid};

pub mod postgres_storage;
pub mod schema;

pub use postgres_storage::PostgresStorage;
use schema::init_tc_schema;

/// Create a PostgresStorage for sync operations.
/// This creates an embedded runtime similar to PostgresTaskManager::new.
pub fn create_postgres_storage(
    database_url: &str,
    user_id: Uuid,
) -> crate::error::Result<PostgresStorage> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| crate::error::TaskError::IoError(e.to_string()))?;

    let tc_uuid = TcUuid::from_bytes(*user_id.as_bytes());

    let storage = rt.block_on(async {
        PostgresStorage::new(database_url.to_string(), tc_uuid)
            .await
            .map_err(|e| {
                crate::error::TaskError::StorageError(format!(
                    "Failed to create PostgresStorage: {}",
                    e
                ))
            })
    })?;

    Ok(storage)
}

#[allow(clippy::too_many_arguments)]
fn compute_urgency(
    is_active: bool,
    is_waiting: bool,
    has_next: bool,
    priority: &Option<String>,
    due: Option<&DateTime<Utc>>,
    entry: Option<&DateTime<Utc>>,
    tags: &[String],
    project: &Option<String>,
) -> f64 {
    let due_score = if let Some(d) = due {
        let days_overdue = (Utc::now() - *d).num_seconds() as f64 / 86400.0;
        if days_overdue >= 7.0 {
            12.0
        } else if days_overdue >= -14.0 {
            12.0 * (((days_overdue + 14.0) * 0.8 / 21.0) + 0.2)
        } else {
            12.0 * 0.2
        }
    } else {
        0.0
    };
    let age_score = if let Some(e) = entry {
        let age_days = (Utc::now() - *e).num_seconds() as f64 / 86400.0;
        (age_days / 365.0).clamp(0.0, 1.0) * 2.0
    } else {
        0.0
    };
    let tag_score = match tags.len() {
        0 => 0.0,
        1 => 0.8,
        2 => 0.9,
        _ => 1.0,
    };
    15.0 * if has_next { 1.0 } else { 0.0 }
        + due_score
        + 6.0
            * if priority.as_deref() == Some("H") {
                1.0
            } else {
                0.0
            }
        + 4.0 * if is_active { 1.0 } else { 0.0 }
        + 3.9
            * if priority.as_deref() == Some("M") {
                1.0
            } else {
                0.0
            }
        + age_score
        + 1.8
            * if priority.as_deref() == Some("L") {
                1.0
            } else {
                0.0
            }
        + tag_score
        + 1.0 * if project.is_some() { 1.0 } else { 0.0 }
        + (-3.0) * if is_waiting { 1.0 } else { 0.0 }
}

pub struct PostgresTaskManager {
    client: Arc<Client>,
    user_id: Uuid,
    rt: Arc<Runtime>,
    database_url: String,
}

impl PostgresTaskManager {
    /// Resolve database URL from explicit value or DATABASE_URL env var.
    fn resolve_database_url(explicit: Option<String>) -> Result<String> {
        match explicit {
            Some(url) if !url.is_empty() => Ok(url),
            _ => std::env::var("DATABASE_URL").map_err(|e| {
                TaskError::StorageError(format!(
                    "DATABASE_URL env var required when database_url not provided: {}",
                    e
                ))
            }),
        }
    }

    /// Create from an already-connected client and a shared runtime.
    /// The runtime must be the same one used to establish the connection
    /// and to run the connection driver task.
    pub fn new_with_runtime(client: Client, user_id: &str, rt: Arc<Runtime>) -> Result<Self> {
        let database_url = Self::resolve_database_url(None)?;
        Self::new_with_runtime_with_url(client, user_id, rt, database_url)
    }

    /// Create from an already-connected client and a shared runtime, with explicit database URL.
    pub fn new_with_runtime_with_url(
        client: Client,
        user_id: &str,
        rt: Arc<Runtime>,
        database_url: String,
    ) -> Result<Self> {
        let user_uuid = Uuid::parse_str(user_id)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid user_id: {}", e)))?;
        Ok(PostgresTaskManager {
            client: Arc::new(client),
            user_id: user_uuid,
            rt,
            database_url,
        })
    }

    pub fn new_arc(client: Arc<Client>, user_id: &str, rt: Arc<Runtime>) -> Result<Self> {
        let database_url = Self::resolve_database_url(None)?;
        Self::new_arc_with_url(client, user_id, rt, database_url)
    }

    /// Create with an Arc<Client>, explicit database URL.
    pub fn new_arc_with_url(
        client: Arc<Client>,
        user_id: &str,
        rt: Arc<Runtime>,
        database_url: String,
    ) -> Result<Self> {
        let user_uuid = Uuid::parse_str(user_id)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid user_id: {}", e)))?;
        Ok(PostgresTaskManager {
            client,
            user_id: user_uuid,
            rt,
            database_url,
        })
    }

    pub fn new(client: Client, user_id: &str) -> Result<Self> {
        let database_url = Self::resolve_database_url(None)?;
        Self::new_with_url(client, user_id, database_url)
    }

    /// Create with explicit database URL.
    pub fn new_with_url(client: Client, user_id: &str, database_url: String) -> Result<Self> {
        let user_uuid = Uuid::parse_str(user_id)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid user_id: {}", e)))?;
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?,
        );
        Ok(PostgresTaskManager {
            client: Arc::new(client),
            user_id: user_uuid,
            rt,
            database_url,
        })
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS tg_tasks (
                    user_id UUID NOT NULL,
                    uuid UUID NOT NULL,
                    description TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    project TEXT,
                    tags TEXT[] NOT NULL DEFAULT '{}',
                    priority TEXT,
                    entry TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    due TIMESTAMPTZ,
                    wait TIMESTAMPTZ,
                    start TIMESTAMPTZ,
                    recur TEXT,
                    urgency DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    is_active BOOLEAN NOT NULL DEFAULT false,
                    is_waiting BOOLEAN NOT NULL DEFAULT false,
                    PRIMARY KEY (user_id, uuid)
                )",
                &[],
            )
            .await
            .map_err(|e| TaskError::StorageError(format!("Failed to create tasks table: {e}")))?;

        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS tg_working_set (
                    user_id UUID NOT NULL,
                    task_uuid UUID NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (user_id, task_uuid)
                )",
                &[],
            )
            .await
            .map_err(|e| {
                TaskError::StorageError(format!("Failed to create working_set table: {e}"))
            })?;

        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS tg_sync_config (
                    user_id UUID PRIMARY KEY,
                    server_url TEXT,
                    client_id TEXT,
                    encryption_secret_encrypted TEXT,
                    last_sync TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )",
                &[],
            )
            .await
            .map_err(|e| {
                TaskError::StorageError(format!("Failed to create sync_config table: {e}"))
            })?;

        Ok(())
    }

    pub fn create_task(&mut self, description: &str) -> Result<TaskInfo> {
        if description.is_empty() {
            return Err(TaskError::InvalidDescription(
                "Description cannot be empty".to_string(),
            ));
        }

        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let user_id = self.user_id;
        let description = description.to_string();

        self.rt.block_on(async {
            self.client
                .execute(
                    "INSERT INTO tg_tasks (user_id, uuid, description, status, entry, modified_at, urgency)
                     VALUES ($1, $2, $3, 'pending', $4, $4, 0)",
                    &[&user_id, &uuid, &description, &now],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to create task: {e}")))?;

            // Add to working set
            self.client
                .execute(
                    "INSERT INTO tg_working_set (user_id, task_uuid, position)
                     SELECT $1, $2, COALESCE(MAX(position), 0) + 1 FROM tg_working_set WHERE user_id = $1",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to add to working set: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        self.get_task(&uuid.to_string())?
            .ok_or_else(|| TaskError::TaskNotFound(uuid.to_string()))
    }

    pub fn get_task(&mut self, uuid_str: &str) -> Result<Option<TaskInfo>> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        let result = self.rt.block_on(async {
            let row = self.client
                .query_opt(
                    "SELECT uuid, description, status, project, tags, priority, entry, modified_at, due, wait, start, recur, urgency, is_active, is_waiting
                     FROM tg_tasks WHERE user_id = $1 AND uuid = $2 AND status != 'deleted'",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to get task: {e}")))?;

            Ok::<Option<TaskInfo>, TaskError>(row.map(|r| Self::row_to_task_info(&r)))
        })?;

        Ok(result)
    }

    pub fn list_tasks(&mut self) -> Result<Vec<TaskInfo>> {
        let user_id = self.user_id;

        let rows = self.rt.block_on(async {
            let rows = self.client
                .query(
                    "SELECT uuid, description, status, project, tags, priority, entry, modified_at, due, wait, start, recur, urgency, is_active, is_waiting
                     FROM tg_tasks WHERE user_id = $1 ORDER BY modified_at DESC",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to list tasks: {e}")))?;
            Ok::<Vec<TaskInfo>, TaskError>(rows.into_iter().map(|r| Self::row_to_task_info(&r)).collect())
        })?;

        Ok(rows)
    }

    pub fn list_tasks_filtered(&mut self, filter: TaskFilter) -> Result<Vec<TaskInfo>> {
        let all = self.list_tasks()?;

        let filtered: Vec<TaskInfo> = all
            .into_iter()
            .filter(|t| {
                if let Some(ref status) = filter.status {
                    if t.status != *status {
                        return false;
                    }
                }
                if let Some(ref project) = filter.project {
                    if t.project.as_deref() != Some(project) {
                        return false;
                    }
                }
                if let Some(ref tag) = filter.tag {
                    if !t.tags.contains(tag) {
                        return false;
                    }
                }
                true
            })
            .collect();

        Ok(filtered)
    }

    pub fn get_working_set(&mut self) -> Result<Vec<WorkingSetItem>> {
        let user_id = self.user_id;

        let items = self.rt.block_on(async {
            let rows = self.client
                .query(
                    "SELECT t.uuid, t.description, t.status, t.project, t.tags, t.priority, t.entry, t.modified_at, t.due, t.wait, t.start, t.recur, t.urgency, t.is_active, t.is_waiting
                     FROM tg_working_set ws
                     JOIN tg_tasks t ON ws.user_id = t.user_id AND ws.task_uuid = t.uuid
                     WHERE ws.user_id = $1
                     ORDER BY ws.position",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to get working set: {e}")))?;

            Ok::<Vec<WorkingSetItem>, TaskError>(
                rows.iter()
                    .enumerate()
                    .map(|(i, r)| WorkingSetItem {
                        id: i as u64,
                        task: Self::row_to_task_info(r),
                    })
                    .collect()
            )
        })?;

        Ok(items)
    }

    pub fn update_task(&mut self, uuid_str: &str, updates: TaskUpdate) -> Result<TaskInfo> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        self.rt.block_on(async {
            if let Some(ref description) = updates.description {
                if description.is_empty() {
                    return Err(TaskError::InvalidDescription("Description cannot be empty".to_string()));
                }
                self.client
                    .execute(
                        "UPDATE tg_tasks SET description = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                        &[description, &user_id, &uuid],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
            }

            if let Some(ref project) = updates.project {
                if project.is_empty() {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET project = NULL, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                            &[&user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                } else {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET project = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[project, &user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }
            }

            if let Some(ref tags) = updates.tags {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET tags = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                        &[tags, &user_id, &uuid],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
            }

            if let Some(ref priority) = updates.priority {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET priority = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                        &[priority, &user_id, &uuid],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
            }

            if let Some(ref due) = updates.due {
                if due.is_empty() {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET due = NULL, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                            &[&user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                } else {
                    let dt: DateTime<Utc> = DateTime::parse_from_rfc3339(due)
                        .map_err(|e| TaskError::InvalidDate(e.to_string()))?
                        .into();
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET due = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[&dt, &user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }
            }

            if let Some(ref wait) = updates.wait {
                if wait.is_empty() {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET wait = NULL, is_waiting = false, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                            &[&user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                } else {
                    let dt: DateTime<Utc> = DateTime::parse_from_rfc3339(wait)
                        .map_err(|e| TaskError::InvalidDate(e.to_string()))?
                        .into();
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET wait = $1, is_waiting = true, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[&dt, &user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }
            }

            if let Some(ref recur) = updates.recur {
                if recur.is_empty() {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET recur = NULL, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                            &[&user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                } else {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET recur = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[recur, &user_id, &uuid],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }
            }

            Ok::<(), TaskError>(())
        })?;

        self.get_task(uuid_str)?
            .ok_or_else(|| TaskError::TaskNotFound(uuid_str.to_string()))
    }

    pub fn complete_task(&mut self, uuid_str: &str) -> Result<TaskInfo> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute(
                    "UPDATE tg_tasks SET status = 'completed', modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to complete: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        self.get_task(uuid_str)?
            .ok_or_else(|| TaskError::TaskNotFound(uuid_str.to_string()))
    }

    pub fn uncomplete_task(&mut self, uuid_str: &str) -> Result<TaskInfo> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute(
                    "UPDATE tg_tasks SET status = 'pending', modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to uncomplete: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        self.get_task(uuid_str)?
            .ok_or_else(|| TaskError::TaskNotFound(uuid_str.to_string()))
    }

    pub fn delete_task(&mut self, uuid_str: &str) -> Result<()> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute(
                    "UPDATE tg_tasks SET status = 'deleted', modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to delete: {e}")))?;

            // Remove from working set
            self.client
                .execute(
                    "DELETE FROM tg_working_set WHERE user_id = $1 AND task_uuid = $2",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to remove from working set: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        Ok(())
    }

    pub fn configure_sync(
        &mut self,
        server_url: &str,
        encryption_secret: &str,
        client_id: &str,
    ) -> Result<()> {
        if server_url.is_empty() {
            return Err(TaskError::InvalidSyncUrl(
                "Server URL cannot be empty".to_string(),
            ));
        }

        if !server_url.starts_with("http://") && !server_url.starts_with("https://") {
            return Err(TaskError::InvalidSyncUrl(
                "Server URL must start with http:// or https://".to_string(),
            ));
        }

        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute(
                    "INSERT INTO tg_sync_config (user_id, server_url, client_id, encryption_secret_encrypted, modified_at)
                     VALUES ($1, $2, $3, $4, NOW())
                     ON CONFLICT (user_id) DO UPDATE SET server_url = $2, client_id = $3, encryption_secret_encrypted = $4, modified_at = NOW()",
                    &[&user_id, &server_url, &client_id, &encryption_secret],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to configure sync: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        Ok(())
    }

    pub fn get_sync_config(&mut self) -> Result<Option<(Option<String>, Option<String>)>> {
        let user_id = self.user_id;
        let result = self.rt.block_on(async {
            let row = self
                .client
                .query_opt(
                    "SELECT server_url, client_id FROM tg_sync_config WHERE user_id = $1",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to get sync config: {e}")))?;
            Ok::<Option<(Option<String>, Option<String>)>, TaskError>(row.map(|r| {
                let server_url: Option<String> = r.get("server_url");
                let client_id: Option<String> = r.get("client_id");
                (server_url, client_id)
            }))
        })?;
        Ok(result)
    }

    pub fn sync(&mut self) -> Result<SyncResult> {
        let user_id = self.user_id;

        let row = self.rt.block_on(async {
            self.client
                .query_opt(
                    "SELECT server_url, client_id, encryption_secret_encrypted \
                     FROM tg_sync_config WHERE user_id = $1",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to read sync config: {e}")))
        })?;

        let row = match row {
            Some(r) => r,
            None => {
                return Ok(SyncResult {
                    success: false,
                    message: "Sync not configured".to_string(),
                })
            }
        };

        let server_url: Option<String> = row.get("server_url");
        let client_id_str: Option<String> = row.get("client_id");
        let secret_str: Option<String> = row.get("encryption_secret_encrypted");

        let (server_url, client_id_str, secret_str) = match (server_url, client_id_str, secret_str)
        {
            (Some(u), Some(c), Some(s)) => (u, c, s),
            _ => {
                return Ok(SyncResult {
                    success: false,
                    message: "Sync config incomplete (missing url, client_id, or secret)"
                        .to_string(),
                })
            }
        };

        let tc_client_id = TcUuid::parse_str(&client_id_str)
            .map_err(|e| TaskError::StorageError(format!("Invalid client_id UUID: {e}")))?;

        let pg_tasks = self.rt.block_on(async {
            self.client
                .query(
                    "SELECT uuid, description, status, project, tags, priority, \
                            entry, modified_at, due, wait, start, recur, is_active \
                     FROM tg_tasks WHERE user_id = $1",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to read tasks: {e}")))
        })?;

        let result = self.rt.block_on(async {
            init_tc_schema(&self.client).await.map_err(|e| {
                taskchampion::Error::Database(format!("Failed to init schema: {}", e))
            })?;

            let tc_user_id = TcUuid::from_bytes(*user_id.as_bytes());
            let storage = PostgresStorage::new(self.database_url.clone(), tc_user_id)
                .await
                .map_err(|e| {
                    taskchampion::Error::Database(format!("Failed to create storage: {}", e))
                })?;
            let mut replica = Replica::new(storage);

            for row in &pg_tasks {
                let uuid_val: Uuid = row.get("uuid");
                let tc_uuid = TcUuid::from_bytes(*uuid_val.as_bytes());

                let mut ops = Operations::new();
                let mut task = replica.create_task(tc_uuid, &mut ops).await?;

                let desc: String = row.get("description");
                task.set_description(desc, &mut ops)?;

                let status_str: String = row.get("status");
                let tc_status = match status_str.as_str() {
                    "completed" => Status::Completed,
                    "deleted" => Status::Deleted,
                    "recurring" => Status::Recurring,
                    _ => Status::Pending,
                };
                task.set_status(tc_status, &mut ops)?;

                if let Some(project) = row.get::<_, Option<String>>("project") {
                    task.set_value("project", Some(project), &mut ops)?;
                }

                let tags: Vec<String> = row.get("tags");
                for tag_str in tags {
                    if let Ok(tag) = tag_str.parse::<Tag>() {
                        task.add_tag(&tag, &mut ops)?;
                    }
                }

                let priority: Option<String> = row.get("priority");
                if let Some(p) = priority {
                    if !p.is_empty() {
                        task.set_priority(p, &mut ops)?;
                    }
                }

                let entry: Option<DateTime<Utc>> = row.get("entry");
                if let Some(e) = entry {
                    task.set_entry(Some(e), &mut ops)?;
                }

                let due: Option<DateTime<Utc>> = row.get("due");
                task.set_due(due, &mut ops)?;

                let wait: Option<DateTime<Utc>> = row.get("wait");
                task.set_wait(wait, &mut ops)?;

                if let Some(recur) = row.get::<_, Option<String>>("recur") {
                    task.set_value("recur", Some(recur), &mut ops)?;
                }

                let is_active: bool = row.get("is_active");
                if is_active {
                    task.set_value(
                        "start",
                        Some(chrono::Utc::now().timestamp().to_string()),
                        &mut ops,
                    )?;
                }

                replica.commit_operations(ops).await?;
            }

            let server_config = ServerConfig::Remote {
                url: server_url.clone(),
                client_id: tc_client_id,
                encryption_secret: secret_str.into_bytes(),
            };
            let mut server = server_config.into_server().await?;
            replica.sync(&mut server, false).await?;

            let all_uuids = replica.all_task_uuids().await?;
            let mut synced_tasks: Vec<(TcUuid, taskchampion::Task)> = Vec::new();
            for uuid in all_uuids {
                if let Some(task) = replica.get_task(uuid).await? {
                    synced_tasks.push((uuid, task));
                }
            }

            Ok::<Vec<(TcUuid, taskchampion::Task)>, taskchampion::Error>(synced_tasks)
        });

        let synced_tasks = match result {
            Ok(tasks) => tasks,
            Err(e) => {
                return Ok(SyncResult {
                    success: false,
                    message: format!("Sync failed: {e}"),
                })
            }
        };

        self.rt.block_on(async {
            self.client
                .execute("DELETE FROM tg_tasks WHERE user_id = $1", &[&user_id])
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to clear tasks before sync write: {e}")))?;

            for (tc_uuid, task) in &synced_tasks {
                let task_uuid = Uuid::from_bytes(*tc_uuid.as_bytes());
                let desc = task.get_description().to_string();
                let status_str = format!("{:?}", task.get_status()).to_lowercase();
                let project: Option<String> = task.get_value("project").map(|s| s.to_string());
                let tags: Vec<String> = task.get_tags()
                    .filter(|t| !t.is_synthetic())
                    .map(|t| t.to_string())
                    .collect();
                let priority: Option<String> = {
                    let p = task.get_priority();
                    if p.is_empty() { None } else { Some(p.to_string()) }
                };
                let entry: Option<DateTime<Utc>> = task.get_entry();
                let modified_at: DateTime<Utc> = task.get_modified().unwrap_or_else(Utc::now);
                let due: Option<DateTime<Utc>> = task.get_due();
                let wait: Option<DateTime<Utc>> = task.get_wait();
                let is_active = task.is_active();
                let is_waiting = task.is_waiting();
                let start: Option<DateTime<Utc>> = task.get_value("start")
                    .and_then(|s| s.parse::<i64>().ok())
                    .map(|ts| DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now));
                let recur: Option<String> = task.get_value("recur").map(|s| s.to_string());

                let has_next = tags.iter().any(|t| t == "next");
                let urgency = compute_urgency(
                    is_active,
                    is_waiting,
                    has_next,
                    &priority,
                    due.as_ref(),
                    entry.as_ref(),
                    &tags,
                    &project,
                );

                self.client
                    .execute(
                        "INSERT INTO tg_tasks \
                         (user_id, uuid, description, status, project, tags, priority, \
                          entry, modified_at, due, wait, start, recur, urgency, is_active, is_waiting) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                         ON CONFLICT (user_id, uuid) DO UPDATE SET \
                           description = EXCLUDED.description, \
                           status = EXCLUDED.status, \
                           project = EXCLUDED.project, \
                           tags = EXCLUDED.tags, \
                           priority = EXCLUDED.priority, \
                           entry = EXCLUDED.entry, \
                           modified_at = EXCLUDED.modified_at, \
                           due = EXCLUDED.due, \
                           wait = EXCLUDED.wait, \
                           start = EXCLUDED.start, \
                           recur = EXCLUDED.recur, \
                           urgency = EXCLUDED.urgency, \
                           is_active = EXCLUDED.is_active, \
                           is_waiting = EXCLUDED.is_waiting",
                        &[
                            &user_id, &task_uuid, &desc, &status_str, &project, &tags,
                            &priority, &entry, &modified_at, &due, &wait, &start,
                            &recur, &urgency, &is_active, &is_waiting,
                        ],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to upsert synced task: {e}")))?;
            }

            self.client
                .execute(
                    "UPDATE tg_sync_config SET last_sync = NOW() WHERE user_id = $1",
                    &[&user_id],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to update last_sync: {e}")))?;

            Ok::<(), TaskError>(())
        })?;

        Ok(SyncResult {
            success: true,
            message: format!("Synced {} tasks", synced_tasks.len()),
        })
    }

    pub fn clear_local_data(&mut self) -> Result<()> {
        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute("DELETE FROM tg_tasks WHERE user_id = $1", &[&user_id])
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to clear: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        Ok(())
    }

    pub fn start_task(&mut self, uuid_str: &str) -> Result<TaskInfo> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;
        let now = Utc::now();

        self.rt.block_on(async {
            self.client
                .execute(
                    "UPDATE tg_tasks SET is_active = true, start = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                    &[&now, &user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to start: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        self.get_task(uuid_str)?
            .ok_or_else(|| TaskError::TaskNotFound(uuid_str.to_string()))
    }

    pub fn stop_task(&mut self, uuid_str: &str) -> Result<TaskInfo> {
        let uuid = Uuid::parse_str(uuid_str)
            .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
        let user_id = self.user_id;

        self.rt.block_on(async {
            self.client
                .execute(
                    "UPDATE tg_tasks SET is_active = false, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                    &[&user_id, &uuid],
                )
                .await
                .map_err(|e| TaskError::StorageError(format!("Failed to stop: {e}")))?;
            Ok::<(), TaskError>(())
        })?;

        self.get_task(uuid_str)?
            .ok_or_else(|| TaskError::TaskNotFound(uuid_str.to_string()))
    }

    pub fn list_tasks_sorted(
        &mut self,
        filter: TaskFilter,
        sort_by: SortField,
    ) -> Result<Vec<TaskInfo>> {
        let mut tasks = self.list_tasks_filtered(filter)?;

        match sort_by {
            SortField::Urgency => tasks.sort_by(|a, b| {
                b.urgency
                    .partial_cmp(&a.urgency)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortField::DueDate => tasks.sort_by(|a, b| match (&a.due, &b.due) {
                (Some(a_due), Some(b_due)) => a_due.cmp(b_due),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }),
            SortField::Priority => tasks.sort_by(|a, b| {
                let a_pri = priority_order(&a.priority);
                let b_pri = priority_order(&b.priority);
                b_pri.cmp(&a_pri)
            }),
            SortField::EntryDate => tasks.sort_by(|a, b| match (&a.entry, &b.entry) {
                (Some(a_entry), Some(b_entry)) => a_entry.cmp(b_entry),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }),
            SortField::Modified => tasks.sort_by(|a, b| match (&a.modified, &b.modified) {
                (Some(a_mod), Some(b_mod)) => a_mod.cmp(b_mod),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }),
            SortField::Description => tasks.sort_by(|a, b| a.description.cmp(&b.description)),
        }

        Ok(tasks)
    }

    pub fn user_id(&self) -> String {
        self.user_id.to_string()
    }

    fn row_to_task_info(row: &tokio_postgres::Row) -> TaskInfo {
        let uuid: Uuid = row.get("uuid");
        let description: String = row.get("description");
        let status: String = row.get("status");
        let project: Option<String> = row.get("project");
        let tags: Vec<String> = row.get("tags");
        let priority: Option<String> = row.get("priority");
        let entry: Option<DateTime<Utc>> = row.get("entry");
        let modified: Option<DateTime<Utc>> = row.get("modified_at");
        let due: Option<DateTime<Utc>> = row.get("due");
        let wait: Option<DateTime<Utc>> = row.get("wait");
        let start: Option<DateTime<Utc>> = row.get("start");
        let recur: Option<String> = row.get("recur");
        let urgency: f64 = row.get("urgency");
        let is_active: bool = row.get("is_active");
        let is_waiting: bool = row.get("is_waiting");

        TaskInfo {
            uuid: uuid.to_string(),
            description,
            status,
            project,
            tags,
            priority,
            entry: entry.map(|dt| dt.to_rfc3339()),
            modified: modified.map(|dt| dt.to_rfc3339()),
            due: due.map(|dt| dt.to_rfc3339()),
            wait: wait.map(|dt| dt.to_rfc3339()),
            start: start.map(|dt| dt.to_rfc3339()),
            recur,
            urgency,
            is_active,
            is_waiting,
        }
    }
}

fn priority_order(priority: &Option<String>) -> u8 {
    match priority.as_deref() {
        Some("H") => 3,
        Some("M") => 2,
        Some("L") => 1,
        _ => 0,
    }
}
