mod error;
mod mask;
mod replication;
mod database;

pub use database::mongodb::{
    destination::MongodbDestination,
    destination_builder::MongodbDestinationBuilder,
    source::MongodbSource,
    source_builder::MongodbSourceBuilder,
};
pub use error::{TuxedoError, TuxedoResult};
pub use mask::Mask;
pub use replication::{
    manager::ReplicationManager,
    manager_builder::ReplicationManagerBuilder,
    processor::ProcessorConfigBuilder,
    types::ReplicationStrategy,
};
