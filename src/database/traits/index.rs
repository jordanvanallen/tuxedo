use crate::database::index::{IndexConfig, SourceIndexes};
use crate::{TuxedoError, TuxedoResult};

pub trait SourceIndexManager {
    async fn list_indexes(&self, entity_name: &str) -> TuxedoResult<SourceIndexes>;
}

pub trait DestinationIndexManager {
    async fn create_index(&self, config: &IndexConfig) -> TuxedoResult<()>;
    async fn drop_index(&self, index_name: &str) -> TuxedoResult<()>;
    async fn create_indexes(&self, source_indexes: SourceIndexes) -> TuxedoResult<()> {
        for index_config in source_indexes.indexes.into_iter() {
            self
                .create_index(&index_config)
                .await
                .map_err(|e| TuxedoError::IndexError(
                    format!("Failed to create index using configuration: {:?} - Error: {:?}", &index_config, e),
                ))?;
        }
        Ok(())
    }
}