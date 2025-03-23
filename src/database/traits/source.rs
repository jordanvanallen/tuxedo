use crate::database::traits::{ConnectionTestable, ReadOperations};
use crate::TuxedoResult;

pub trait Source: ReadOperations + ConnectionTestable {
    async fn prepare_database(&self) -> TuxedoResult<()>;
}
