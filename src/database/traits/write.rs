use crate::TuxedoResult;

pub trait WriteOperations {
    async fn write<T, O>(
        &self,
        entity_name: &str,
        options: O,
        records: &Vec<T>,
    ) -> TuxedoResult<()>
    where
        T: serde::Serialize + Send + Sync;
}
