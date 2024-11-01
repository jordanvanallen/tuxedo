pub mod mongodb_model;
pub mod result;

// Reexport the derive(MongoModel) and associated attribute macros when users include mongo_model with "derive" in the
// features list.
#[cfg(feature = "derive")]
pub use mongodb_model_derive::MongoModel;

pub use crate::{
    mongodb_model::MongoModel,
    result::{MongoDbModelError, Result},
};
