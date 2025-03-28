use crate::database::traits::{index::SourceIndexManager, ConnectionTestable, ReadOperations};
use crate::TuxedoResult;
use async_trait::async_trait;

#[async_trait]
pub trait Source
where
    Self: ReadOperations
    + SourceIndexManager
    + ConnectionTestable
    + Send
    + Sync,
{
    async fn prepare_database(&self) -> TuxedoResult<()>;
}
