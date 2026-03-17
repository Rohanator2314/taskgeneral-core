//! Postgres storage backend for TaskGeneral.

#[cfg(feature = "postgres")]
pub mod pg {
    use crate::error::{Result, TaskError};
    use crate::models::{SortField, SyncResult, TaskFilter, TaskInfo, TaskUpdate, WorkingSetItem};
    use chrono::{DateTime, Utc};
    use tokio_postgres::Client;
    use uuid::Uuid;

    pub struct PostgresTaskManager {
        client: Client,
        user_id: Uuid,
    }

    impl PostgresTaskManager {
        pub fn new(client: Client, user_id: &str) -> Result<Self> {
            let user_uuid = Uuid::parse_str(user_id).map_err(|e| {
                TaskError::InvalidUuid(format!("Invalid user_id: {}", e))
            })?;

            Ok(PostgresTaskManager {
                client,
                user_id: user_uuid,
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
            let user_id_str = self.user_id.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "INSERT INTO tg_tasks (user_id, uuid, description, status, entry, modified_at, urgency)
                         VALUES ($1, $2, $3, 'pending', $4, $4, 0)",
                        &[&user_id_str, &uuid.to_string(), &description.to_string(), &now.to_rfc3339()],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            let result = runtime.block_on(async {
                let row = self.client
                    .query_opt(
                        "SELECT uuid, description, status, project, tags, priority, entry, modified_at, due, wait, start, recur, urgency, is_active, is_waiting
                         FROM tg_tasks WHERE user_id = $1 AND uuid = $2",
                        &[&user_id_str, &uuid_str_owned],
                    )
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to get task: {e}")))?;

                Ok::<Option<TaskInfo>, TaskError>(row.map(|r| Self::row_to_task_info(&r)))
            })?;

            Ok(result)
        }

        pub fn list_tasks(&mut self) -> Result<Vec<TaskInfo>> {
            let user_id_str = self.user_id.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            let rows = runtime.block_on(async {
                let rows = self.client
                    .query(
                        "SELECT uuid, description, status, project, tags, priority, entry, modified_at, due, wait, start, recur, urgency, is_active, is_waiting
                         FROM tg_tasks WHERE user_id = $1 ORDER BY modified_at DESC",
                        &[&user_id_str],
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
            let user_id_str = self.user_id.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            let items = runtime.block_on(async {
                let rows = self.client
                    .query(
                        "SELECT t.uuid, t.description, t.status, t.project, t.tags, t.priority, t.entry, t.modified_at, t.due, t.wait, t.start, t.recur, t.urgency, t.is_active, t.is_waiting
                         FROM tg_working_set ws
                         JOIN tg_tasks t ON ws.user_id = t.user_id AND ws.task_uuid = t.uuid
                         WHERE ws.user_id = $1
                         ORDER BY ws.position",
                        &[&user_id_str],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                if let Some(ref description) = updates.description {
                    if description.is_empty() {
                        return Err(TaskError::InvalidDescription("Description cannot be empty".to_string()));
                    }
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET description = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[description, &user_id_str, &uuid_str_owned],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }

                if let Some(ref project) = updates.project {
                    if project.is_empty() {
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET project = NULL, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                                &[&user_id_str, &uuid_str_owned],
                            )
                            .await
                            .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                    } else {
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET project = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                                &[project, &user_id_str, &uuid_str_owned],
                            )
                            .await
                            .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                    }
                }

                if let Some(ref tags) = updates.tags {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET tags = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[tags, &user_id_str, &uuid_str_owned],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }

                if let Some(ref priority) = updates.priority {
                    self.client
                        .execute(
                            "UPDATE tg_tasks SET priority = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                            &[priority, &user_id_str, &uuid_str_owned],
                        )
                        .await
                        .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                }

                if let Some(ref due) = updates.due {
                    if due.is_empty() {
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET due = NULL, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                                &[&user_id_str, &uuid_str_owned],
                            )
                            .await
                            .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                    } else {
                        let dt = DateTime::parse_from_rfc3339(due)
                            .map_err(|e| TaskError::InvalidDate(e.to_string()))?;
                        let dt_str = dt.to_rfc3339();
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET due = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                                &[&dt_str, &user_id_str, &uuid_str_owned],
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
                                &[&user_id_str, &uuid_str_owned],
                            )
                            .await
                            .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                    } else {
                        let dt = DateTime::parse_from_rfc3339(wait)
                            .map_err(|e| TaskError::InvalidDate(e.to_string()))?;
                        let dt_str = dt.to_rfc3339();
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET wait = $1, is_waiting = true, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                                &[&dt_str, &user_id_str, &uuid_str_owned],
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
                                &[&user_id_str, &uuid_str_owned],
                            )
                            .await
                            .map_err(|e| TaskError::StorageError(format!("Failed to update: {e}")))?;
                    } else {
                        self.client
                            .execute(
                                "UPDATE tg_tasks SET recur = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                                &[recur, &user_id_str, &uuid_str_owned],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET status = 'completed', modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                        &[&user_id_str, &uuid_str_owned],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET status = 'pending', modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                        &[&user_id_str, &uuid_str_owned],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "DELETE FROM tg_tasks WHERE user_id = $1 AND uuid = $2",
                        &[&user_id_str, &uuid_str_owned],
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

            let user_id_str = self.user_id.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "INSERT INTO tg_sync_config (user_id, server_url, client_id, encryption_secret_encrypted, modified_at)
                         VALUES ($1, $2, $3, $4, NOW())
                         ON CONFLICT (user_id) DO UPDATE SET server_url = $2, client_id = $3, encryption_secret_encrypted = $4, modified_at = NOW()",
                        &[&user_id_str, &server_url.to_string(), &client_id.to_string(), &encryption_secret.to_string()],
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
            let user_id_str = self.user_id.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute("DELETE FROM tg_tasks WHERE user_id = $1", &[&user_id_str])
                    .await
                    .map_err(|e| TaskError::StorageError(format!("Failed to clear: {e}")))?;
                Ok::<(), TaskError>(())
            })?;

            Ok(())
        }

        pub fn start_task(&mut self, uuid_str: &str) -> Result<TaskInfo> {
            let uuid = Uuid::parse_str(uuid_str)
                .map_err(|e| TaskError::InvalidUuid(format!("Invalid UUID: {}", e)))?;
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();
            let now = Utc::now().to_rfc3339();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET is_active = true, start = $1, modified_at = NOW() WHERE user_id = $2 AND uuid = $3",
                        &[&now, &user_id_str, &uuid_str_owned],
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
            let user_id_str = self.user_id.to_string();
            let uuid_str_owned = uuid.to_string();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TaskError::IoError(e.to_string()))?;

            runtime.block_on(async {
                self.client
                    .execute(
                        "UPDATE tg_tasks SET is_active = false, modified_at = NOW() WHERE user_id = $1 AND uuid = $2",
                        &[&user_id_str, &uuid_str_owned],
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
            let uuid: String = row.get("uuid");
            let description: String = row.get("description");
            let status: String = row.get("status");
            let project: Option<String> = row.get("project");
            let tags: Vec<String> = row.get("tags");
            let priority: Option<String> = row.get("priority");
            let entry: Option<String> = row.get("entry");
            let modified: Option<String> = row.get("modified_at");
            let due: Option<String> = row.get("due");
            let wait: Option<String> = row.get("wait");
            let start: Option<String> = row.get("start");
            let recur: Option<String> = row.get("recur");
            let urgency: f64 = row.get("urgency");
            let is_active: bool = row.get("is_active");
            let is_waiting: bool = row.get("is_waiting");

            TaskInfo {
                uuid,
                description,
                status,
                project,
                tags,
                priority,
                entry,
                modified,
                due,
                wait,
                start,
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
