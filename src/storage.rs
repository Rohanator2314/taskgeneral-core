//! Postgres storage backend for TaskGeneral.

#[cfg(feature = "postgres")]
pub mod pg;

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use crate::models::{SortField, TaskFilter, TaskUpdate};
    use crate::storage::pg::PostgresTaskManager;
    use native_tls::TlsConnector;
    use postgres_native_tls::MakeTlsConnector;
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    /// Build one runtime + connect one client.  Both the driver task and all
    /// subsequent `block_on` calls share the same runtime, so the connection
    /// never closes underneath us.
    fn connect() -> (tokio_postgres::Client, Arc<Runtime>, String) {
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

        rt.spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });

        (client, rt, url)
    }

    fn setup() -> PostgresTaskManager {
        let (client, rt, database_url) = connect();
        let user_id = Uuid::new_v4().to_string();
        let mgr = PostgresTaskManager::new_with_runtime_with_url(
            client,
            &user_id,
            rt.clone(),
            database_url,
        )
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
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    description: Some("Updated".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.description, "Updated");
    }

    #[test]
    fn test_update_empty_description_fails() {
        let mut mgr = setup();
        let task = mgr.create_task("Original").unwrap();
        let result = mgr.update_task(
            &task.uuid,
            TaskUpdate {
                description: Some("".to_string()),
                ..Default::default()
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_update_project() {
        let mut mgr = setup();
        let task = mgr.create_task("With project").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    project: Some("work".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.project.as_deref(), Some("work"));

        let cleared = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    project: Some("".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(cleared.project.is_none());
    }

    #[test]
    fn test_update_tags() {
        let mut mgr = setup();
        let task = mgr.create_task("Tagged task").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    tags: Some(vec!["urgent".to_string(), "home".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(updated.tags.contains(&"urgent".to_string()));
        assert!(updated.tags.contains(&"home".to_string()));
    }

    #[test]
    fn test_update_priority() {
        let mut mgr = setup();
        let task = mgr.create_task("Priority task").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    priority: Some("H".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.priority.as_deref(), Some("H"));
    }

    #[test]
    fn test_update_due_date() {
        let mut mgr = setup();
        let task = mgr.create_task("Due task").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    due: Some("2026-12-31T00:00:00+00:00".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(updated.due.is_some());

        let cleared = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    due: Some("".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(cleared.due.is_none());
    }

    #[test]
    fn test_update_wait_date() {
        let mut mgr = setup();
        let task = mgr.create_task("Wait task").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    wait: Some("2026-06-01T00:00:00+00:00".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(updated.wait.is_some());
        assert!(updated.is_waiting);

        let cleared = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    wait: Some("".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(cleared.wait.is_none());
        assert!(!cleared.is_waiting);
    }

    #[test]
    fn test_update_recur() {
        let mut mgr = setup();
        let task = mgr.create_task("Recurring task").unwrap();
        let updated = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    recur: Some("weekly".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.recur.as_deref(), Some("weekly"));

        let cleared = mgr
            .update_task(
                &task.uuid,
                TaskUpdate {
                    recur: Some("".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(cleared.recur.is_none());
    }

    #[test]
    fn test_update_invalid_due_date_fails() {
        let mut mgr = setup();
        let task = mgr.create_task("Bad date").unwrap();
        let result = mgr.update_task(
            &task.uuid,
            TaskUpdate {
                due: Some("not-a-date".to_string()),
                ..Default::default()
            },
        );
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
        let uncomplete = mgr.uncomplete_task(&task.uuid).unwrap();
        assert_eq!(uncomplete.status, "pending");
    }

    // ================================================================
    // delete_task
    // ================================================================

    #[test]
    fn test_delete_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Delete me").unwrap();
        mgr.delete_task(&task.uuid).unwrap();
        let result = mgr.get_task(&task.uuid).unwrap();
        assert!(result.is_none());
    }

    // ================================================================
    // start_task / stop_task
    // ================================================================

    #[test]
    fn test_start_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Start me").unwrap();
        assert!(!task.is_active);
        let started = mgr.start_task(&task.uuid).unwrap();
        assert!(started.is_active);
    }

    #[test]
    fn test_stop_task() {
        let mut mgr = setup();
        let task = mgr.create_task("Stop me").unwrap();
        mgr.start_task(&task.uuid).unwrap();
        let stopped = mgr.stop_task(&task.uuid).unwrap();
        assert!(!stopped.is_active);
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

    #[test]
    fn test_get_working_set_includes_tasks() {
        let mut mgr = setup();
        mgr.create_task("Task 1").unwrap();
        mgr.create_task("Task 2").unwrap();
        let ws = mgr.get_working_set().unwrap();
        assert_eq!(ws.len(), 2);
    }

    // ================================================================
    // list_tasks_sorted
    // ================================================================

    #[test]
    fn test_list_tasks_sorted_by_urgency() {
        let mut mgr = setup();
        mgr.create_task("Low urgency").unwrap();
        let high_task = mgr.create_task("High urgency").unwrap();
        let high = mgr
            .update_task(
                &high_task.uuid,
                TaskUpdate {
                    priority: Some("H".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let tasks = mgr
            .list_tasks_sorted(TaskFilter::default(), SortField::Urgency)
            .unwrap();
        assert_eq!(tasks[0].uuid, high.uuid);
    }

    // ================================================================
    // configure_sync / get_sync_config
    // ================================================================

    #[test]
    fn test_configure_sync() {
        let mut mgr = setup();
        mgr.configure_sync(
            "https://sync.example.com",
            "secret123",
            &Uuid::new_v4().to_string(),
        )
        .unwrap();
        let config = mgr.get_sync_config().unwrap();
        assert!(config.is_some());
    }

    // ================================================================
    // sync
    // ================================================================

    #[test]
    fn test_sync_not_configured() {
        let mut mgr = setup();
        let result = mgr.sync().unwrap();
        assert!(!result.success);
        assert!(result.message.contains("not configured"));
    }

    // ================================================================
    // clear_local_data
    // ================================================================

    #[test]
    fn test_clear_local_data() {
        let mut mgr = setup();
        mgr.create_task("Task to clear").unwrap();
        mgr.clear_local_data().unwrap();
        let tasks = mgr.list_tasks().unwrap();
        assert!(tasks.is_empty());
    }

    // ================================================================
    // user_id
    // ================================================================

    #[test]
    fn test_user_id() {
        let mgr = setup();
        let uid = mgr.user_id();
        assert!(Uuid::parse_str(&uid).is_ok());
    }

    // ================================================================
    // filtered listing
    // ================================================================

    #[test]
    fn test_list_tasks_filtered_by_status() {
        let mut mgr = setup();
        mgr.create_task("Pending task").unwrap();
        let completed = mgr.create_task("Done task").unwrap();
        mgr.complete_task(&completed.uuid).unwrap();

        let pending = mgr
            .list_tasks_filtered(TaskFilter {
                status: Some("pending".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(pending.len(), 1);

        let done = mgr
            .list_tasks_filtered(TaskFilter {
                status: Some("completed".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn test_list_tasks_filtered_by_project() {
        let mut mgr = setup();
        let work_task = mgr.create_task("Work task").unwrap();
        mgr.update_task(
            &work_task.uuid,
            TaskUpdate {
                project: Some("work".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        mgr.create_task("Home task").unwrap();

        let work = mgr
            .list_tasks_filtered(TaskFilter {
                project: Some("work".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].project.as_deref(), Some("work"));
    }

    #[test]
    fn test_list_tasks_filtered_by_tag() {
        let mut mgr = setup();
        let tagged = mgr.create_task("Tagged").unwrap();
        mgr.update_task(
            &tagged.uuid,
            TaskUpdate {
                tags: Some(vec!["important".to_string()]),
                ..Default::default()
            },
        )
        .unwrap();
        mgr.create_task("Untagged").unwrap();

        let with_tag = mgr
            .list_tasks_filtered(TaskFilter {
                tag: Some("important".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(with_tag.len(), 1);
    }

    #[test]
    fn test_pg_storage_e2e_sync_incremental_operations() {
        // Check if E2E_TEST is set
        if std::env::var("E2E_TEST").ok() != Some("1".into()) {
            println!("Skipping E2E test (set E2E_TEST=1 to run)");
            return;
        }

        // This test requires:
        // - TEST_DATABASE_URL set
        // - E2E_TEST=1
        // - A running taskchampion-sync-server on localhost:8081
        // The test validates that the sync pipeline works end-to-end
        // with PostgresStorage backing the Replica.

        // Future implementation will:
        // 1. Create a PostgresTaskManager with a test database
        // 2. Configure sync with a real sync server
        // 3. Create tasks and sync to server
        // 4. Create a second PostgresTaskManager instance
        // 5. Sync from server and verify tasks are replicated
        // 6. Make modifications on both sides
        // 7. Sync and verify conflict resolution works

        println!("E2E sync test placeholder - requires sync server implementation");
    }
}
