use thiserror::Error;

pub type Result<T> = std::result::Result<T, MongoDbModelError>;

#[derive(Debug, Error)]
/// The unified error type for MongoDB Model methods.
pub enum MongoDbModelError {
    /// A generic underlying error from the mongodb driver functions.
    #[error("{0}")]
    MongoDBError(#[from] mongodb::error::Error),

    /// Issues with generation and parsing of BSON OIDs.
    #[error("{0}")]
    BsonOidError(#[from] bson::oid::Error),

    #[error("Record missing from database while trying to sync.")]
    SyncRecordMissing,

    #[error("Model instance requires an ID in order for deletes to work.")]
    ModelIdMissingOnDelete,

    #[error("Model insert returned unexpected ID type back from the database")]
    MongoDBInvalidIdTypeAfterInsert,

    #[error("Collection {collection} did not contain record with query: `{query}`")]
    RecordNotFound {
        collection: &'static str,
        query: bson::Document,
    },
}
