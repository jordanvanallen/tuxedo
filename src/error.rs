use thiserror::Error;
use tokio::sync::AcquireError;

#[derive(Debug, Error)]
pub enum TuxedoError {
    #[error("std::io::Error: {0}")]
    StdIoError(#[from] std::io::Error),

    #[error("Task error: {0}")]
    TaskError(String),

    #[error("Masking Config Error: {0}")]
    ConfigError(String),

    #[error("Database driver error: {0}")]
    Database(#[from] mongodb::error::Error),

    #[error("Error when acquiring semaphore: {0}")]
    SemaphoreError(#[from] AcquireError),

    #[error("Error serializing data: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Error joining future: {0}")]
    FutureJoin(#[from] tokio::task::JoinError),

    #[error("Error sending task to worker pool: {0}")]
    WorkerSend(
        #[from] tokio::sync::mpsc::error::SendError<Box<dyn crate::replication::task::Task>>,
    ),

    #[error("Generic flagged error: {0}")]
    #[allow(dead_code)]
    Generic(String),

    #[error("Uncaught Error type")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type TuxedoResult<T> = std::result::Result<T, TuxedoError>;
