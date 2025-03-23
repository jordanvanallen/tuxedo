use crate::TuxedoResult;

use super::connection_testable::ConnectionTestable;
use super::write::WriteOperations;

pub trait Destination: WriteOperations + ConnectionTestable {
    async fn prepare_database(&self) -> TuxedoResult<()>;
    async fn clear_database(&self, entity_names: &[String]) -> TuxedoResult<()>;
}
