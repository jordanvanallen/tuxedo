use crate::database::index::SourceIndexes;
use crate::TuxedoResult;
use async_trait::async_trait;

#[async_trait]
pub trait SourceIndexManager {
    async fn list_indexes(&self, entity_name: &str) -> TuxedoResult<SourceIndexes>;
}

#[async_trait]
pub trait DestinationIndexManager {
    // async fn create_index(&self, config: &IndexConfig) -> TuxedoResult<()>;
    async fn drop_index(&self, entity_name: &str, index_name: &str) -> TuxedoResult<()>;

    async fn create_indexes(&self, source_indexes: SourceIndexes) -> TuxedoResult<()>;
}