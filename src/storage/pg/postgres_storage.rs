use async_trait::async_trait;
use taskchampion::storage::{Storage, StorageTxn, TaskMap};
use taskchampion::server::VersionId;
use taskchampion::{Error as TcError, Operation, Uuid};
use tokio_postgres::{Client, NoTls};

pub struct PostgresStorage {
    pub database_url: String,
    pub client: Client,
    pub user_id: Uuid,
}

impl PostgresStorage {
    pub async fn new(database_url: String, user_id: Uuid) -> Result<Self, TcError> {
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
            .await
            .map_err(|e| TcError::Database(format!("Failed to connect to database: {}", e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(PostgresStorage { 
            database_url,
            client,
            user_id,
        })
    }

    pub fn from_client(client: Client, database_url: String, user_id: Uuid) -> Self {
        PostgresStorage {
            database_url,
            client,
            user_id,
        }
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn txn<'a>(&'a mut self) -> std::result::Result<Box<dyn StorageTxn + Send + 'a>, TcError> {
        Ok(Box::new(PostgresTxn::new(&self.client, self.user_id)))
    }
}

pub struct PostgresTxn<'a> {
    client: &'a Client,
    user_id: Uuid,
}

impl<'a> PostgresTxn<'a> {
    pub fn new(client: &'a Client, user_id: Uuid) -> Self {
        PostgresTxn { client, user_id }
    }
}

#[async_trait]
impl<'a> StorageTxn for PostgresTxn<'a> {
    async fn get_task(&mut self, uuid: Uuid) -> std::result::Result<Option<TaskMap>, TcError> {
        let query = "SELECT task_map FROM tc_tasks WHERE user_id = $1 AND uuid = $2";
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        
        let result = self.client
            .query_opt(query, &[&user_uuid, &task_uuid])
            .await
            .map_err(|e| TcError::Database(format!("Failed to get task: {}", e)))?;

        match result {
            Some(row) => {
                let task_json: serde_json::Value = row.get(0);
                let task_map: TaskMap = serde_json::from_value(task_json)
                    .map_err(|e| TcError::Database(format!("Failed to deserialize task: {}", e)))?;
                Ok(Some(task_map))
            }
            None => Ok(None),
        }
    }

    async fn get_pending_tasks(&mut self) -> std::result::Result<Vec<(Uuid, TaskMap)>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let rows = self.client
            .query(
                "SELECT t.uuid, t.task_map 
                 FROM tc_tasks t 
                 INNER JOIN tg_working_set ws ON t.uuid = ws.task_uuid AND t.user_id = ws.user_id 
                 WHERE t.user_id = $1 
                 ORDER BY ws.position",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query get_pending_tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let task_uuid: uuid::Uuid = row.get("uuid");
            let uuid = Uuid::from_bytes(*task_uuid.as_bytes());
            let task_map_json: serde_json::Value = row.get("task_map");
            
            let task_map: TaskMap = serde_json::from_value(task_map_json)
                .map_err(|e| TcError::Database(format!("Failed to deserialize task_map: {}", e)))?;
            
            tasks.push((uuid, task_map));
        }

        Ok(tasks)
    }

    async fn create_task(&mut self, uuid: Uuid) -> std::result::Result<bool, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        let empty_task_map = serde_json::json!({});

        let result = self.client
            .execute(
                "INSERT INTO tc_tasks (user_id, uuid, task_map) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                &[&user_uuid, &task_uuid, &empty_task_map],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to create task: {}", e)))?;

        Ok(result > 0)
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        let task_json = serde_json::to_value(&task)
            .map_err(|e| TcError::Database(format!("Failed to serialize task: {}", e)))?;

        self.client
            .execute(
                "INSERT INTO tc_tasks (user_id, uuid, task_map) VALUES ($1, $2, $3)
                 ON CONFLICT (user_id, uuid) DO UPDATE SET task_map = $3",
                &[&user_uuid, &task_uuid, &task_json],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to set task: {}", e)))?;

        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> std::result::Result<bool, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());

        let result = self.client
            .execute(
                "DELETE FROM tc_tasks WHERE user_id = $1 AND uuid = $2",
                &[&user_uuid, &task_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to delete task: {}", e)))?;

        Ok(result > 0)
    }

    async fn all_tasks(&mut self) -> std::result::Result<Vec<(Uuid, TaskMap)>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let rows = self.client
            .query(
                "SELECT uuid, task_map FROM tc_tasks WHERE user_id = $1",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query all_tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let task_uuid: uuid::Uuid = row.get("uuid");
            let uuid = Uuid::from_bytes(*task_uuid.as_bytes());
            let task_map_json: serde_json::Value = row.get("task_map");
            
            let task_map: TaskMap = serde_json::from_value(task_map_json)
                .map_err(|e| TcError::Database(format!("Failed to deserialize task_map: {}", e)))?;
            
            tasks.push((uuid, task_map));
        }

        Ok(tasks)
    }

    async fn all_task_uuids(&mut self) -> std::result::Result<Vec<Uuid>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let rows = self.client
            .query(
                "SELECT uuid FROM tc_tasks WHERE user_id = $1",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query all_task_uuids: {}", e)))?;

        let mut uuids = Vec::new();
        for row in rows {
            let task_uuid: uuid::Uuid = row.get("uuid");
            let uuid = Uuid::from_bytes(*task_uuid.as_bytes());
            uuids.push(uuid);
        }

        Ok(uuids)
    }

    async fn base_version(&mut self) -> std::result::Result<VersionId, TcError> {
        let row = self.client
            .query_opt(
                "SELECT value FROM sync_meta WHERE user_id = $1 AND key = 'base_version'",
                &[&self.user_id],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query base_version: {}", e)))?;

        match row {
            Some(row) => {
                let value_str: String = row.get("value");
                // Parse the UUID string back to a VersionId (which is a Uuid type alias)
                let version_uuid = uuid::Uuid::parse_str(&value_str)
                    .map_err(|_| TcError::Database("Invalid base_version value".to_string()))?;
                Ok(version_uuid)
            }
            None => Ok(uuid::Uuid::nil()),
        }
    }

    async fn set_base_version(&mut self, version: VersionId) -> std::result::Result<(), TcError> {
        let version_str = version.to_string();
        
        self.client
            .execute(
                "INSERT INTO sync_meta (user_id, key, value) VALUES ($1, $2, $3)
                 ON CONFLICT (user_id, key) DO UPDATE SET value = $3",
                &[&self.user_id, &"base_version", &version_str],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to set base_version: {}", e)))?;

        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> std::result::Result<Vec<Operation>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        
        let rows = self.client
            .query(
                "SELECT data FROM operations WHERE user_id = $1 AND uuid = $2 ORDER BY id",
                &[&user_uuid, &task_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to get task operations: {}", e)))?;

        let mut operations = Vec::new();
        for row in rows {
            let data: serde_json::Value = row.get("data");
            let operation: Operation = serde_json::from_value(data)
                .map_err(|e| TcError::Database(format!("Failed to deserialize operation: {}", e)))?;
            operations.push(operation);
        }

        Ok(operations)
    }

    async fn unsynced_operations(&mut self) -> std::result::Result<Vec<Operation>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let rows = self.client
            .query(
                "SELECT data FROM operations WHERE user_id = $1 AND synced = false ORDER BY id",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to get unsynced operations: {}", e)))?;

        let mut operations = Vec::new();
        for row in rows {
            let data: serde_json::Value = row.get("data");
            let operation: Operation = serde_json::from_value(data)
                .map_err(|e| TcError::Database(format!("Failed to deserialize operation: {}", e)))?;
            operations.push(operation);
        }

        Ok(operations)
    }

    async fn num_unsynced_operations(&mut self) -> std::result::Result<usize, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let row = self.client
            .query_one(
                "SELECT COUNT(*) as count FROM operations WHERE user_id = $1 AND synced = false",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to count unsynced operations: {}", e)))?;

        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    async fn add_operation(&mut self, op: Operation) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let task_uuid = match &op {
            Operation::Create { uuid } => uuid::Uuid::from_bytes(*uuid.as_bytes()),
            Operation::Delete { uuid, .. } => uuid::Uuid::from_bytes(*uuid.as_bytes()),
            Operation::Update { uuid, .. } => uuid::Uuid::from_bytes(*uuid.as_bytes()),
            Operation::UndoPoint => uuid::Uuid::nil(),
        };
        
        let data = serde_json::to_value(&op)
            .map_err(|e| TcError::Database(format!("Failed to serialize operation: {}", e)))?;
        
        self.client
            .execute(
                "INSERT INTO operations (user_id, uuid, data, synced) VALUES ($1, $2, $3, false)",
                &[&user_uuid, &task_uuid, &data],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to add operation: {}", e)))?;

        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        let data = serde_json::to_value(&op)
            .map_err(|e| TcError::Database(format!("Failed to serialize operation: {}", e)))?;
        
        self.client
            .execute(
                "DELETE FROM operations WHERE user_id = $1 AND data = $2",
                &[&user_uuid, &data],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to remove operation: {}", e)))?;

        Ok(())
    }

    async fn sync_complete(&mut self) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        self.client
            .execute(
                "UPDATE operations SET synced = true WHERE user_id = $1 AND synced = false",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to mark operations as synced: {}", e)))?;

        Ok(())
    }

    async fn get_working_set(&mut self) -> std::result::Result<Vec<Option<Uuid>>, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let rows = self.client
            .query(
                "SELECT task_uuid, position FROM tg_working_set WHERE user_id = $1 ORDER BY position",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query working set: {}", e)))?;
        
        // Start with None at index 0 for 1-based indexing
        let mut result = vec![None];
        
        for row in rows {
            let task_uuid: uuid::Uuid = row.get("task_uuid");
            let position: i32 = row.get("position");
            
            // Convert uuid::Uuid to taskchampion::Uuid
            let tc_uuid = Uuid::from_bytes(*task_uuid.as_bytes());
            
            // Ensure the vector is large enough for this position
            while result.len() <= position as usize {
                result.push(None);
            }
            
            result[position as usize] = Some(tc_uuid);
        }
        
        Ok(result)
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> std::result::Result<usize, TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let task_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        
        // Get the maximum position
        let max_position_row = self.client
            .query_opt(
                "SELECT COALESCE(MAX(position), 0) as max_pos FROM tg_working_set WHERE user_id = $1",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to query max position: {}", e)))?;
        
        let max_position: i32 = max_position_row
            .map(|row| row.get("max_pos"))
            .unwrap_or(0);
        
        let new_position = max_position + 1;
        
        // Insert the new item
        self.client
            .execute(
                "INSERT INTO tg_working_set (user_id, task_uuid, position) VALUES ($1, $2, $3)
                 ON CONFLICT (user_id, task_uuid) DO NOTHING",
                &[&user_uuid, &task_uuid, &new_position],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to add to working set: {}", e)))?;
        
        Ok(new_position as usize)
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        let position = index as i32;
        
        match uuid {
            Some(task_id) => {
                let task_uuid = uuid::Uuid::from_bytes(*task_id.as_bytes());
                
                // First, check if there's an existing item at this position
                let existing = self.client
                    .query_opt(
                        "SELECT task_uuid FROM tg_working_set WHERE user_id = $1 AND position = $2",
                        &[&user_uuid, &position],
                    )
                    .await
                    .map_err(|e| TcError::Database(format!("Failed to check existing working set item: {}", e)))?;
                
                if let Some(_) = existing {
                    // Update existing item
                    self.client
                        .execute(
                            "UPDATE tg_working_set SET task_uuid = $3 WHERE user_id = $1 AND position = $2",
                            &[&user_uuid, &position, &task_uuid],
                        )
                        .await
                        .map_err(|e| TcError::Database(format!("Failed to update working set item: {}", e)))?;
                } else {
                    // Insert new item
                    self.client
                        .execute(
                            "INSERT INTO tg_working_set (user_id, task_uuid, position) VALUES ($1, $2, $3)",
                            &[&user_uuid, &task_uuid, &position],
                        )
                        .await
                        .map_err(|e| TcError::Database(format!("Failed to insert working set item: {}", e)))?;
                }
            }
            None => {
                // Delete the item at this position
                self.client
                    .execute(
                        "DELETE FROM tg_working_set WHERE user_id = $1 AND position = $2",
                        &[&user_uuid, &position],
                    )
                    .await
                    .map_err(|e| TcError::Database(format!("Failed to delete working set item: {}", e)))?;
            }
        }
        
        Ok(())
    }

    async fn clear_working_set(&mut self) -> std::result::Result<(), TcError> {
        let user_uuid = uuid::Uuid::from_bytes(*self.user_id.as_bytes());
        
        self.client
            .execute(
                "DELETE FROM tg_working_set WHERE user_id = $1",
                &[&user_uuid],
            )
            .await
            .map_err(|e| TcError::Database(format!("Failed to clear working set: {}", e)))?;
        
        Ok(())
    }

    async fn commit(&mut self) -> std::result::Result<(), TcError> {
        // PostgresStorage uses individual auto-committed queries rather than explicit transactions.
        // Each query is automatically committed, so this is a no-op.
        Ok(())
    }
}
