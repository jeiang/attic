//! Database migrations.

pub use sea_orm_migration::*;

mod m20221227_000001_create_cache_table;
mod m20221227_000002_create_nar_table;
mod m20221227_000003_create_object_table;
mod m20221227_000004_add_object_last_accessed;
mod m20221227_000005_add_cache_retention_period;
mod m20230103_000001_add_object_created_by;
mod m20230112_000001_add_chunk_table;
mod m20230112_000002_add_chunkref_table;
mod m20230112_000003_add_nar_num_chunks;
mod m20230112_000004_migrate_nar_remote_files_to_chunks;
mod m20230112_000005_drop_old_nar_columns;
mod m20230112_000006_add_nar_completeness_hint;
mod m20260508_000001_add_chunk_state_holders_index;
mod m20260611_000001_add_nar_state_holders_index;
mod m20260624_000001_remove_chunk_recovery;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20221227_000001_create_cache_table::Migration),
            Box::new(m20221227_000002_create_nar_table::Migration),
            Box::new(m20221227_000003_create_object_table::Migration),
            Box::new(m20221227_000004_add_object_last_accessed::Migration),
            Box::new(m20221227_000005_add_cache_retention_period::Migration),
            Box::new(m20230103_000001_add_object_created_by::Migration),
            Box::new(m20230112_000001_add_chunk_table::Migration),
            Box::new(m20230112_000002_add_chunkref_table::Migration),
            Box::new(m20230112_000003_add_nar_num_chunks::Migration),
            Box::new(m20230112_000004_migrate_nar_remote_files_to_chunks::Migration),
            Box::new(m20230112_000005_drop_old_nar_columns::Migration),
            Box::new(m20230112_000006_add_nar_completeness_hint::Migration),
            Box::new(m20260508_000001_add_chunk_state_holders_index::Migration),
            Box::new(m20260611_000001_add_nar_state_holders_index::Migration),
            Box::new(m20260624_000001_remove_chunk_recovery::Migration),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use sea_orm::SqlxSqliteConnector;
    use sea_orm::sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };

    use super::{Migrator, MigratorTrait};

    /// The full migration chain must succeed on a fresh file-backed SQLite
    /// database, using the same connect options as `connect_sqlite` and the
    /// single connection that `run_migrations` enforces. With more than one
    /// pooled connection, consecutive migration statements can land on
    /// different connections and observe stale schema state, failing
    /// nondeterministically.
    #[tokio::test]
    async fn full_migration_chain_on_fresh_sqlite_file() {
        let dir = std::env::temp_dir().join(format!(
            "attic-migration-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("sqlite://{}/server.db?mode=rwc", dir.display());

        let connect_options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("temp_store", "memory");

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .unwrap();
        let db = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);

        Migrator::up(&db, None).await.unwrap();

        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
