use crate::TuxedoResult;
use async_trait::async_trait;

#[async_trait]
pub trait ConnectionTestable {
    async fn test_database_connection(&self) -> TuxedoResult<()>;
}