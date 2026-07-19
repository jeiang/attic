use sea_orm_migration::prelude::*;

use crate::database::entity::cache::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260719_000001_add_cache_deleted_at_index"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name("idx-cache-deleted-at")
                    .table(Entity)
                    .col(Column::DeletedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx-cache-deleted-at")
                    .table(Entity)
                    .to_owned(),
            )
            .await
    }
}
