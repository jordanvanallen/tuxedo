use mongodb::{Client, Database, options::InsertManyOptions};
use mongodb::options::ClientOptions;
use serde::Serialize;

use crate::database::traits::{ConnectionTestable, Destination, WriteOperations};
use crate::TuxedoResult;

pub struct MongodbDestination {
    client: Client,
    db: Database,
}

impl MongodbDestination {
    pub async fn new<O>(db_name: &str, options: O) -> TuxedoResult<Self>
    where
        O: Into<ClientOptions>,
    {
        // TODO: Should we be helping here set things like pool sizes based on threads?
        let client = Client::with_options(options.into())?;
        let db = client.database(db_name);

        Ok(Self { client, db })
    }
}

impl WriteOperations for MongodbDestination {
    async fn write<T, O>(
        &self,
        collection_name: &str,
        options: O,
        records: &Vec<T>,
    ) -> TuxedoResult<()>
    where
        T: Serialize + Send + Sync,
        O: Into<Option<InsertManyOptions>>,
    {
        self.db
            .collection::<T>(collection_name)
            .insert_many(records)
            .with_options(options.into())
            .await?;
        Ok(())
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
        self.client.warm_connection_pool();
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
                self.target
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
