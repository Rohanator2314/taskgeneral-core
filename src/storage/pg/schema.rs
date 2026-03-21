//! SQL migrations for TaskChampion storage backend

/// Schema initialization SQL for TaskChampion storage tables.
/// All tables include user_id UUID NOT NULL for multi-user isolation.
pub const INIT_SCHEMA_SQL: &str = r#"
-- tc_tasks: TaskChampion task storage with JSONB task map
CREATE TABLE IF NOT EXISTS tc_tasks (
    user_id UUID NOT NULL,
    uuid UUID NOT NULL,
    task_map JSONB NOT NULL,
    PRIMARY KEY (user_id, uuid)
);
CREATE INDEX IF NOT EXISTS idx_tc_tasks_user_uuid ON tc_tasks (user_id, uuid);

-- operations: Operation log for incremental sync
CREATE TABLE IF NOT EXISTS operations (
    id BIGSERIAL,
    user_id UUID NOT NULL,
    uuid UUID NOT NULL,
    data JSONB,
    synced BOOLEAN NOT NULL DEFAULT false,
    UNIQUE(id)
);
CREATE INDEX IF NOT EXISTS idx_ops_user_synced ON operations (user_id, synced);
CREATE INDEX IF NOT EXISTS idx_ops_user_uuid ON operations (user_id, uuid);

-- sync_meta: Key-value metadata per user
CREATE TABLE IF NOT EXISTS sync_meta (
    user_id UUID NOT NULL,
    key VARCHAR NOT NULL,
    value TEXT,
    PRIMARY KEY (user_id, key)
);

-- version: Schema version per user
CREATE TABLE IF NOT EXISTS version (
    user_id UUID PRIMARY KEY,
    major INTEGER NOT NULL,
    minor INTEGER NOT NULL
);
"#;

/// Initialize TaskChampion storage schema
pub async fn init_tc_schema(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    client.batch_execute(INIT_SCHEMA_SQL).await
}
