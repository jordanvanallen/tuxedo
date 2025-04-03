use crate::database::traits::{Destination, Source};
use crate::database::DatabasePair;
use crate::TuxedoResult;
use bson::Bson;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

// Generalized index definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    Unique,
    Text,
    Geo2DSphere,
    Geo2D,   // Regular 2D geospatial index
    Hashed,
    Compound,
    Partial,
    Standard,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexDirection {
    Ascending,
    Descending,
}

impl From<&IndexDirection> for Bson {
    fn from(direction: &IndexDirection) -> Self {
        match direction {
            IndexDirection::Ascending => Bson::Int32(1),
            IndexDirection::Descending => Bson::Int32(-1),
        }
    }
}

impl From<&Bson> for IndexDirection {
    fn from(bson: &Bson) -> Self {
        match bson {
            Bson::Int32(1) => IndexDirection::Ascending,
            Bson::Int32(-1) => IndexDirection::Descending,
            _ => IndexDirection::Ascending,
        }
    }
}

impl Display for IndexDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => write!(f, "asc"),
            Self::Descending => write!(f, "desc"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexField {
    pub name: String,
    pub direction: IndexDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    pub fields: Vec<IndexField>,
    pub index_type: IndexType,
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIndexes {
    pub entity_name: String,
    pub indexes: Vec<IndexConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct IndexManager {}

impl IndexManager {
    pub(crate) async fn copy_indexes<S, D>(
        dbs: &DatabasePair<S, D>,
        entity_name: &str,
    ) -> TuxedoResult<()>
    where
        S: Source,
        D: Destination,
    {
        let indexes = dbs
            .source
            .list_indexes(entity_name)
            .await?;
        dbs
            .destination
            .create_indexes(indexes)
            .await
    }
}