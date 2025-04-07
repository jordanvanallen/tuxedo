use crate::database::index::SourceIndexes;
use crate::database::mongodb::destination_builder::MongodbDestinationBuilder;
use crate::database::traits::{ConnectionTestable, Destination, DestinationIndexManager, WriteOperations};
use crate::TuxedoResult;
use async_trait::async_trait;
use bson::Document;
use mongodb::{options::InsertManyOptions, Client, Database, IndexModel};
use serde::Serialize;

pub struct MongodbDestination {
    client: Client,
    db: Database,
    write_options: Option<InsertManyOptions>,
}

impl MongodbDestination {
    pub fn builder() -> MongodbDestinationBuilder {
        MongodbDestinationBuilder::new()
    }

    pub fn new(client: Client, db: Database, write_options: Option<InsertManyOptions>) -> Self
    {
        Self { client, db, write_options }
    }
}

#[async_trait]
impl WriteOperations for MongodbDestination {
    type WriteOptions = InsertManyOptions;

    async fn write<T>(
        &self,
        collection_name: &str,
        records: &[T],
    ) -> TuxedoResult<()>
    where
        T: Serialize + Send + Sync,
    {
        self.db
            .collection::<T>(collection_name)
            .insert_many(records)
            .with_options(self.write_options.clone())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl DestinationIndexManager for MongodbDestination {
    async fn drop_index(&self, collection_name: &str, index_name: &str) -> TuxedoResult<()> {
        self
            .db
            .collection::<Document>(collection_name)
            .drop_index(index_name)
            .await?;
        Ok(())
    }

    async fn create_indexes(&self, source_indexes: SourceIndexes) -> TuxedoResult<()> {
        let index_models: Vec<IndexModel> = Vec::from(source_indexes.clone());

        if !index_models.is_empty() {
            self.db
                .collection::<Document>(&source_indexes.entity_name)
                .create_indexes(index_models)
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ConnectionTestable for MongodbDestination {
    async fn test_database_connection(&self) -> TuxedoResult<()> {
        self.db.list_collection_names().await?;
        Ok(())
    }
}

#[async_trait]
impl Destination for MongodbDestination {
    async fn prepare_database(&self) -> TuxedoResult<()> {
        // Warm connection pool, the MongoDB driver will 
        // establish connections up to min_pool_size
        self.client.warm_connection_pool().await;
        Ok(())
    }

    async fn clear_database(&self, entity_names: &[String]) -> TuxedoResult<()> {
        let collections = self.db.list_collection_names().await?;

        println!("******************************");
        for collection_name in collections.into_iter() {
            // Skip system collections:
            // 1. Collections with system.* prefix
            // 2. Collections in admin database
            // 3. Collections in config database
            // 4. Special system collections
            //
            // This shouldn't happen in reality unless a library user for
            // whatever reason points to a system collection as their model's
            // collection by accident, but better safe than sorry
            if collection_name.starts_with("system.")
                || collection_name.starts_with("admin.")
                || collection_name.starts_with("config.")
            {
                println!("Skipping system collection: {}", collection_name);
                continue;
            }

            // Only drop collections that are in our processor list
            if entity_names.contains(&collection_name) {
                println!("Dropping collection: {}", collection_name);
                self.db
                    .collection::<mongodb::bson::Document>(&collection_name)
                    .drop()
                    .await?;
            } else {
                println!(
                    "Skipping collection not in processor list: {}",
                    collection_name
                );
            }
        }
        println!("******************************");
        println!("Target database collections have been selectively dropped.\n\n");
        Ok(())
    }
}
