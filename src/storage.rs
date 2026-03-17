//! Postgres storage backend for TaskGeneral.

#[cfg(feature = "postgres")]
pub mod pg {
    use crate::error::{Result, TaskError};
    use crate::models::{SortField, SyncResult, TaskFilter, TaskInfo, TaskUpdate, WorkingSetItem};
    use chrono::{DateTime, Utc};
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use tokio_postgres::Client;
    use uuid::Uuid;

    pub struct PostgresTaskManager {
        client: Client,
        user_id: Uuid,
        rt: Arc<Runtime>,
    }

    impl PostgresTaskManager {
        /// Create from an already-connected client and a shared runtime.
        /// The runtime must be the same one used to establish the connection
        /// and to run the connection driver task.
        pub fn new_with_runtime(client: Client, user_id: &str, rt: Arc<Runtime>) -> Result<Self> {
            let user_uuid = Uuid::parse_str(user_id)
                .map_err(|e| TaskError::InvalidUuid(format!("Invalid user_id: {}", e)))?;
            Ok(PostgresTaskManager {
                client,
                user_id: user_uuid,
                rt,
            })
        }

        /// Backwards-compatible constructor: creates its own single-threaded runtime,
        /// then hands the client to it.  The runtime is stored inside the manager so
        /// every subsequent call uses the same runtime → same event-loop → connection
        /// driver stays alive.
        pub fn new(client: Client, user_id: &str) -> Result<Self> {
            let user_uuid = Uuid::parse_str(user_id)
                .map_err(|e| TaskError::InvalidUuid(format!("Invalid user_id: {}", e)))?;
            let rt = Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| TaskError::IoError(e.to_string()))?,
            );
            Ok(PostgresTaskManager {
                client,
                user_id: user_uuid,
                rt,
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
                .map_err(|e| TaskError::StorageError(format!("Failed to create working_set table: {e}")))?;

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
                .map_err(|e| TaskError::StorageError(format!("Failed to create sync_config table: {e}")))?;

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
                         FROM tg_tasks WHERE user_id = $1 AND uuid = $2",
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
                        "DELETE FROM tg_tasks WHERE user_id = $1 AND uuid = $2",
                        &[&user_id, &uuid],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to delete: {e}")))?;
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

        pub fn sync(&mut self) -> Result<SyncResult> {
            Ok(SyncResult {
                success: false,
                message: "Sync not yet implemented for Postgres backend".to_string(),
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
                    b.urgency.partial_cmp(&a.urgency).unwrap_or(std::cmp::Ordering::Equal)
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
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::pg::PostgresTaskManager;
    use crate::models::{SortField, TaskFilter, TaskUpdate};
    use native_tls::TlsConnector;
    use postgres_native_tls::MakeTlsConnector;
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    /// Build one runtime + connect one client.  Both the driver task and all
    /// subsequent `block_on` calls share the same runtime, so the connection
    /// never closes underneath us.
    fn connect() -> (tokio_postgres::Client, Arc<Runtime>) {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set to run postgres tests");
        let tls = MakeTlsConnector::new(
            TlsConnector::builder()
                .build()
                .expect("Failed to build TLS connector"),
        );

        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build runtime"),
        );

        let (client, connection) = rt
            .block_on(tokio_postgres::connect(&url, tls))
            .expect("Failed to connect to test database");

        // Spawn the driver on the SAME runtime so it is polled whenever we
        // call block_on on that runtime.
        rt.spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });

        (client, rt)
    }

    fn setup() -> PostgresTaskManager {
        let (client, rt) = connect();
        let user_id = Uuid::new_v4().to_string();
        let mgr = PostgresTaskManager::new_with_runtime(client, &user_id, rt.clone())
            .expect("Failed to create manager");
        rt.block_on(mgr.init_schema()).expect("init_schema failed");
        mgr
    }

    // ================================================================
    // create_task
    // ================================================================

    #[test]
    fn test_create_task_basic() {
        let mut mgr = setup();
        let task = mgr.create_task("Buy milk").unwrap();
        assert_eq!(task.description, "Buy milk");
        assert_eq!(task.status, "pending");
        assert!(!task.is_active);
        assert!(!task.is_waiting);
        assert!(task.tags.is_empty());
        assert!(task.project.is_none());
        assert!(task.priority.is_none());
        Uuid::parse_str(&task.uuid).expect("uuid should be valid");
    }

    #[test]
    fn test_create_task_empty_description_fails() {
        let mut mgr = setup();
        let result = mgr.create_task("");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("empty") || msg.contains("Description"));
    }

    // ================================================================
    // get_task
    // ================================================================

    #[test]
    fn test_get_task_returns_created() {
        let mut mgr = setup();
        let created = mgr.create_task("Test get").unwrap();
        let fetched = mgr.get_task(&created.uuid).unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.uuid, created.uuid);
        assert_eq!(fetched.description, "Test get");
    }

    #[test]
    fn test_get_task_missing_returns_none() {
        let mut mgr = setup();
        let result = mgr.get_task(&Uuid::new_v4().to_string()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_task_invalid_uuid_errors() {
        let mut mgr = setup();
        let result = mgr.get_task("not-a-uuid");
        assert!(result.is_err());
    }

    // ================================================================
    // list_tasks
    // ================================================================

    #[test]
    fn test_list_tasks_empty() {
        let mut mgr = setup();
        let tasks = mgr.list_tasks().unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn test_list_tasks_returns_all() {
        let mut mgr = setup();
        mgr.create_task("Task A").unwrap();
        mgr.create_task("Task B").unwrap();
        mgr.create_task("Task C").unwrap();
        let tasks = mgr.list_tasks().unwrap();
        assert_eq!(tasks.len(), 3);
    }

    // ================================================================
    // update_task
    // ================================================================

    #[test]
    fn test_update_description() {
        let mut mgr = setup();
        let task = mgr.create_task("Original").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            description: Some("Updated".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(updated.description, "Updated");
    }

    #[test]
    fn test_update_empty_description_fails() {
        let mut mgr = setup();
        let task = mgr.create_task("Original").unwrap();
        let result = mgr.update_task(&task.uuid, TaskUpdate {
            description: Some("".to_string()),
            ..Default::default()
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_update_project() {
        let mut mgr = setup();
        let task = mgr.create_task("With project").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            project: Some("work".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(updated.project.as_deref(), Some("work"));

        let cleared = mgr.update_task(&task.uuid, TaskUpdate {
            project: Some("".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(cleared.project.is_none());
    }

    #[test]
    fn test_update_tags() {
        let mut mgr = setup();
        let task = mgr.create_task("Tagged task").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            tags: Some(vec!["urgent".to_string(), "home".to_string()]),
            ..Default::default()
        }).unwrap();
        assert!(updated.tags.contains(&"urgent".to_string()));
        assert!(updated.tags.contains(&"home".to_string()));
    }

    #[test]
    fn test_update_priority() {
        let mut mgr = setup();
        let task = mgr.create_task("Priority task").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            priority: Some("H".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(updated.priority.as_deref(), Some("H"));
    }

    #[test]
    fn test_update_due_date() {
        let mut mgr = setup();
        let task = mgr.create_task("Due task").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            due: Some("2026-12-31T00:00:00+00:00".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(updated.due.is_some());

        let cleared = mgr.update_task(&task.uuid, TaskUpdate {
            due: Some("".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(cleared.due.is_none());
    }

    #[test]
    fn test_update_wait_date() {
        let mut mgr = setup();
        let task = mgr.create_task("Wait task").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            wait: Some("2026-06-01T00:00:00+00:00".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(updated.wait.is_some());
        assert!(updated.is_waiting);

        let cleared = mgr.update_task(&task.uuid, TaskUpdate {
            wait: Some("".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(cleared.wait.is_none());
        assert!(!cleared.is_waiting);
    }

    #[test]
    fn test_update_recur() {
        let mut mgr = setup();
        let task = mgr.create_task("Recurring task").unwrap();
        let updated = mgr.update_task(&task.uuid, TaskUpdate {
            recur: Some("weekly".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(updated.recur.as_deref(), Some("weekly"));

        let cleared = mgr.update_task(&task.uuid, TaskUpdate {
            recur: Some("".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(cleared.recur.is_none());
    }

    #[test]
    fn test_update_invalid_due_date_fails() {
        let mut mgr = setup();
        let task = mgr.create_task("Bad date").unwrap();
        let result = mgr.update_task(&task.uuid, TaskUpdate {
            due: Some("not-a-date".to_string()),
            ..Default::default()
        });
        assert!(result.is_err());
    }

    // ================================================================
    // complete_task / uncomplete_task
    // ================================================================

    #[test]
    fn test_complete_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Complete me").unwrap();
        assert_eq!(task.status, "pending");
        let completed = mgr.complete_task(&task.uuid).unwrap();
        assert_eq!(completed.status, "completed");
    }

    #[test]
    fn test_uncomplete_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Complete then undo").unwrap();
        mgr.complete_task(&task.uuid).unwrap();
        let restored = mgr.uncomplete_task(&task.uuid).unwrap();
        assert_eq!(restored.status, "pending");
    }

    // ================================================================
    // delete_task
    // ================================================================

    #[test]
    fn test_delete_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Delete me").unwrap();
        mgr.delete_task(&task.uuid).unwrap();
        let fetched = mgr.get_task(&task.uuid).unwrap();
        assert!(fetched.is_none());
    }

    #[test]
    fn test_delete_nonexistent_task_is_ok() {
        let mut mgr = setup();
        let result = mgr.delete_task(&Uuid::new_v4().to_string());
        assert!(result.is_ok());
    }

    // ================================================================
    // start_task / stop_task
    // ================================================================

    #[test]
    fn test_start_and_stop_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Active task").unwrap();
        assert!(!task.is_active);

        let started = mgr.start_task(&task.uuid).unwrap();
        assert!(started.is_active);
        assert!(started.start.is_some());

        let stopped = mgr.stop_task(&task.uuid).unwrap();
        assert!(!stopped.is_active);
    }

    // ================================================================
    // list_tasks_filtered
    // ================================================================

    #[test]
    fn test_filter_by_status() {
        let mut mgr = setup();
        let t1 = mgr.create_task("Pending task").unwrap();
        let t2 = mgr.create_task("Done task").unwrap();
        mgr.complete_task(&t2.uuid).unwrap();

        let pending = mgr.list_tasks_filtered(TaskFilter {
            status: Some("pending".to_string()),
            ..Default::default()
        }).unwrap();
        assert!(pending.iter().all(|t| t.status == "pending"));
        assert!(pending.iter().any(|t| t.uuid == t1.uuid));
        assert!(!pending.iter().any(|t| t.uuid == t2.uuid));
    }

    #[test]
    fn test_filter_by_project() {
        let mut mgr = setup();
        let t1 = mgr.create_task("Work task").unwrap();
        mgr.update_task(&t1.uuid, TaskUpdate {
            project: Some("work".to_string()),
            ..Default::default()
        }).unwrap();
        mgr.create_task("Personal task").unwrap();

        let work = mgr.list_tasks_filtered(TaskFilter {
            project: Some("work".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].project.as_deref(), Some("work"));
    }

    #[test]
    fn test_filter_by_tag() {
        let mut mgr = setup();
        let t1 = mgr.create_task("Tagged").unwrap();
        mgr.update_task(&t1.uuid, TaskUpdate {
            tags: Some(vec!["important".to_string()]),
            ..Default::default()
        }).unwrap();
        mgr.create_task("Untagged").unwrap();

        let tagged = mgr.list_tasks_filtered(TaskFilter {
            tag: Some("important".to_string()),
            ..Default::default()
        }).unwrap();
        assert_eq!(tagged.len(), 1);
        assert!(tagged[0].tags.contains(&"important".to_string()));
    }

    // ================================================================
    // list_tasks_sorted
    // ================================================================

    #[test]
    fn test_sort_by_description() {
        let mut mgr = setup();
        mgr.create_task("Zebra").unwrap();
        mgr.create_task("Apple").unwrap();
        mgr.create_task("Mango").unwrap();

        let sorted = mgr.list_tasks_sorted(TaskFilter::default(), SortField::Description).unwrap();
        let descs: Vec<&str> = sorted.iter().map(|t| t.description.as_str()).collect();
        let mut expected = descs.clone();
        expected.sort();
        assert_eq!(descs, expected);
    }

    #[test]
    fn test_sort_by_priority() {
        let mut mgr = setup();
        let t_low = mgr.create_task("Low priority").unwrap();
        let t_high = mgr.create_task("High priority").unwrap();
        let t_med = mgr.create_task("Medium priority").unwrap();
        mgr.update_task(&t_low.uuid, TaskUpdate { priority: Some("L".to_string()), ..Default::default() }).unwrap();
        mgr.update_task(&t_high.uuid, TaskUpdate { priority: Some("H".to_string()), ..Default::default() }).unwrap();
        mgr.update_task(&t_med.uuid, TaskUpdate { priority: Some("M".to_string()), ..Default::default() }).unwrap();

        let sorted = mgr.list_tasks_sorted(TaskFilter::default(), SortField::Priority).unwrap();
        assert_eq!(sorted[0].priority.as_deref(), Some("H"));
    }

    #[test]
    fn test_sort_by_due_date() {
        let mut mgr = setup();
        let t1 = mgr.create_task("Due later").unwrap();
        let t2 = mgr.create_task("Due sooner").unwrap();
        mgr.update_task(&t1.uuid, TaskUpdate { due: Some("2027-01-01T00:00:00+00:00".to_string()), ..Default::default() }).unwrap();
        mgr.update_task(&t2.uuid, TaskUpdate { due: Some("2026-06-01T00:00:00+00:00".to_string()), ..Default::default() }).unwrap();

        let sorted = mgr.list_tasks_sorted(TaskFilter {
            status: Some("pending".to_string()),
            ..Default::default()
        }, SortField::DueDate).unwrap();

        let due_tasks: Vec<_> = sorted.iter().filter(|t| t.due.is_some()).collect();
        assert!(due_tasks.len() >= 2);
        assert!(due_tasks[0].due.as_ref().unwrap() < due_tasks[1].due.as_ref().unwrap());
    }

    // ================================================================
    // configure_sync
    // ================================================================

    #[test]
    fn test_configure_sync_valid() {
        let mut mgr = setup();
        let result = mgr.configure_sync(
            "https://sync.example.com",
            "mysecret",
            "client-abc",
        );
        assert!(result.is_ok());

        let result2 = mgr.configure_sync(
            "https://sync2.example.com",
            "newsecret",
            "client-xyz",
        );
        assert!(result2.is_ok());
    }

    #[test]
    fn test_configure_sync_empty_url_fails() {
        let mut mgr = setup();
        let result = mgr.configure_sync("", "secret", "client");
        assert!(result.is_err());
    }

    #[test]
    fn test_configure_sync_invalid_url_scheme_fails() {
        let mut mgr = setup();
        let result = mgr.configure_sync("ftp://sync.example.com", "secret", "client");
        assert!(result.is_err());
    }

    // ================================================================
    // sync (stub)
    // ================================================================

    #[test]
    fn test_sync_returns_not_implemented() {
        let mut mgr = setup();
        let result = mgr.sync().unwrap();
        assert!(!result.success);
        assert!(!result.message.is_empty());
    }

    // ================================================================
    // clear_local_data
    // ================================================================

    #[test]
    fn test_clear_local_data() {
        let mut mgr = setup();
        mgr.create_task("Task 1").unwrap();
        mgr.create_task("Task 2").unwrap();
        assert_eq!(mgr.list_tasks().unwrap().len(), 2);

        mgr.clear_local_data().unwrap();
        assert!(mgr.list_tasks().unwrap().is_empty());
    }

    // ================================================================
    // get_working_set
    // ================================================================

    #[test]
    fn test_get_working_set_empty() {
        let mut mgr = setup();
        let ws = mgr.get_working_set().unwrap();
        assert!(ws.is_empty());
    }

    // ================================================================
    // user isolation
    // ================================================================

    #[test]
    fn test_user_isolation() {
        let mut mgr_a = setup();
        let mut mgr_b = setup();

        mgr_a.create_task("User A task").unwrap();

        let b_tasks = mgr_b.list_tasks().unwrap();
        assert!(b_tasks.is_empty(), "User B should not see User A's tasks");

        mgr_b.create_task("User B task").unwrap();

        let a_tasks = mgr_a.list_tasks().unwrap();
        assert_eq!(a_tasks.len(), 1);
        assert_eq!(a_tasks[0].description, "User A task");
    }

    // ================================================================
    // init_schema idempotency
    // ================================================================

    #[test]
    fn test_init_schema_idempotent() {
        let (client, rt) = connect();
        let user_id = Uuid::new_v4().to_string();
        let mgr = PostgresTaskManager::new_with_runtime(client, &user_id, rt.clone())
            .expect("Failed to create manager");
        rt.block_on(mgr.init_schema()).unwrap();
        rt.block_on(mgr.init_schema()).unwrap();
    }

    // ================================================================
    // user_id accessor
    // ================================================================

    #[test]
    fn test_user_id_accessor() {
        let (client, rt) = connect();
        let user_id_str = Uuid::new_v4().to_string();
        let mgr = PostgresTaskManager::new_with_runtime(client, &user_id_str, rt)
            .expect("Failed to create manager");
        assert_eq!(mgr.user_id(), user_id_str);
    }

    // ================================================================
    // invalid user_id
    // ================================================================

    #[test]
    fn test_new_with_invalid_user_id_fails() {
        let (client, rt) = connect();
        let result = PostgresTaskManager::new_with_runtime(client, "not-a-uuid", rt);
        assert!(result.is_err());
    }
}
