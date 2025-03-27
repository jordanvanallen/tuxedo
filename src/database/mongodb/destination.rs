use crate::database::index::IndexConfig;
use crate::database::mongodb::destination_builder::MongodbDestinationBuilder;
use crate::database::traits::{ConnectionTestable, Destination, DestinationIndexManager, WriteOperations};
use crate::TuxedoResult;
use mongodb::{options::InsertManyOptions, Client, Database};
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

impl WriteOperations for MongodbDestination {
    type WriteOptions = InsertManyOptions;

    async fn write<T>(
        &self,
        collection_name: &str,
        records: &Vec<T>,
        // options: impl Into<Option<Self::WriteOptions>>,
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

impl DestinationIndexManager for MongodbDestination {
    async fn create_index(&self, config: &IndexConfig) -> TuxedoResult<()> {
        todo!()
    }

    async fn drop_index(&self, index_name: &str) -> TuxedoResult<()> {
        todo!()
    }
}

impl ConnectionTestable for MongodbDestination {
    async fn test_database_connection(&self) -> TuxedoResult<()> {
        self.db.list_collection_names().await?;
        Ok(())
    }
}

impl Destination for MongodbDestination {
    async fn prepare_database(&self) -> TuxedoResult<()> {
        self.client.warm_connection_pool().await;
        Ok(())
    }

    async fn clear_database(&self, collection_names: &[String]) -> TuxedoResult<()> {
        let collections = self.db.list_collection_names().await?;

        println!("******************************");
        for collection_name in collections.into_iter() {
            // Skip system collections:
            // 1. Collections with system.* prefix
            // 2. Collections in admin database
            // 3. Collections in config database
            // 4. Special system collections
            if collection_name.starts_with("system.")
                || collection_name.starts_with("admin.")
                || collection_name.starts_with("config.")
                || collection_name.ends_with(".system.roles")
                || collection_name.ends_with(".system.users")
                || collection_name.ends_with(".system.version")
                || collection_name.ends_with(".system.buckets")
                || collection_name.ends_with(".system.profile")
                || collection_name.ends_with(".system.js")
                || collection_name.ends_with(".system.views")
            {
                println!("Skipping system collection: {}", collection_name);
                continue;
            }

            // Only drop collections that are in our processor list
            if collection_names.contains(&collection_name) {
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
