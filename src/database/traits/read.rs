use serde::{Serialize, de::DeserializeOwned};
use crate::TuxedoResult;

pub trait ReadOperations {
    // TODO: Do we need Serialize trait restrictions here also?
    // TODO; This might need to split config into options and query
    async fn read<T: DeserializeOwned + Send + Sync, Q, O>(&self, entity_name: &str, query: Q, options: O) -> TuxedoResult<Vec<T>>;
    async fn count_total_records<T: Send + Sync, Q, O>(&self, entity_name: &str, query: Q, options: O) -> TuxedoResult<u64>;
}