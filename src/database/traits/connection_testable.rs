use crate::TuxedoResult;

pub trait ConnectionTestable {
    async fn test_database_connection(&self) -> TuxedoResult<()>;
}