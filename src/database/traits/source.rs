use crate::database::traits::{index::SourceIndexManager, ConnectionTestable, ReadOperations};
use crate::TuxedoResult;

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
