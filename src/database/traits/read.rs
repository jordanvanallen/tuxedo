use crate::database::pagination::PaginationOptions;
use crate::TuxedoResult;
use serde::de::DeserializeOwned;
use async_trait::async_trait;

#[async_trait]
pub trait ReadOperations {
    type Query: Send + Sync + Clone + Default + std::fmt::Display;
    type ReadOptions: Send + Sync + Clone;
    type RecordCountOptions: Send + Sync + Clone;

    fn build_chunk_read_options(&self, config: &PaginationOptions) -> Self::ReadOptions;

    async fn read_chunk<T>(
        &self,
        entity_name: &str,
        query: Self::Query,
        pagination_options: PaginationOptions,
    ) -> TuxedoResult<Vec<T>>
    where
        T: DeserializeOwned + Send + Sync;

    async fn count_total_records<T>(
        &self,
        entity_name: &str,
        query: Self::Query,
    ) -> TuxedoResult<u64>
    where
        T: Send + Sync;
}