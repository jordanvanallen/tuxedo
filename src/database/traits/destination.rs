use super::{
    connection_testable::ConnectionTestable,
    index::DestinationIndexManager,
    write::WriteOperations,
};
use crate::TuxedoResult;

pub trait Destination
where
    Self: WriteOperations
    + DestinationIndexManager
    + ConnectionTestable
    + Send
    + Sync,
{
    async fn prepare_database(&self) -> TuxedoResult<()>;
    async fn clear_database(&self, entity_names: &[String]) -> TuxedoResult<()>;
}
