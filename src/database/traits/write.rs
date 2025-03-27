use crate::TuxedoResult;

pub trait WriteOperations {
    type WriteOptions: Send + Sync;

    async fn write<T>(
        &self,
        entity_name: &str,
        records: &Vec<T>,
        // options: impl Into<Option<Self::WriteOptions>>,
    ) -> TuxedoResult<()>
    where
        T: serde::Serialize + Send + Sync;
}
