use serde::{Deserialize, Serialize};

/// Strategy for how to handle the data during replication
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ReplicationStrategy {
    /// Clone the source data to the destination without modification
    Clone,
    /// Apply masking to the data before writing to the destination
    Mask,
}

/// Controls how documents are processed during replication
/// 
/// - Batch: Load entire batches into memory before processing (default)
/// - Streaming: Process documents as they arrive, reducing memory usage
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StreamingMode {
    /// Traditional batch processing - load entire batches into memory
    /// 
    /// Better for small to medium collections with small documents.
    Batch,
    
    /// Stream processing - process documents as they arrive
    /// 
    /// Better for very large collections or large documents 
    /// to reduce memory pressure.
    Streaming,
}

impl Default for StreamingMode {
    fn default() -> Self {
        Self::Batch
    }
}

impl TryFrom<String> for ReplicationStrategy {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "clone" => Ok(Self::Clone),
            "mask" => Ok(Self::Mask),
            other => Err(format!(
                "{} is not a supported replication strategy.",
                other
            )),
        }
    }
}
