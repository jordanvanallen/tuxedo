use mongodb::{
    bson::Document,
    Client,
    Database, options::{ClientOptions, CountOptions, FindOptions},
};
use serde::de::DeserializeOwned;

use crate::database::traits::{ConnectionTestable, ReadOperations, Source};
use crate::TuxedoResult;

pub struct MongodbSource {
    client: Client,
    db: Database,
}

impl MongodbSource {
    async fn new<O>(db_name: &str, options: O) -> TuxedoResult<Self>
    where
        O: Into<ClientOptions>,
    {
        // TODO: Should we be helping here set things like pool sizes based on threads?
        let client = Client::with_options(options.into())?;
        let db = client.database(db_name);

        Ok(Self { client, db })
    }
}

impl Source for MongodbSource {
    async fn prepare_database(&self) -> TuxedoResult<()> {
        self.client.warm_connection_pool();
        Ok(())
    }
}

impl ConnectionTestable for MongodbSource {
    async fn test_database_connection(&self) -> TuxedoResult<()> {
        self.db.list_collection_names().await?;
        Ok(())
    }
}

impl ReadOperations for MongodbSource {
    async fn read<T, Q, O>(
        &self,
        collection_name: &str,
        query: Q,
        options: O,
    ) -> TuxedoResult<Vec<T>>
    where
        T: DeserializeOwned + Send + Sync,
        Q: Into<Document>,
        O: Into<Option<FindOptions>>,
    {
        let documents = self
            .db
            .collection::<T>(collection_name)
            .find(query)
            .with_options(options)
            .await?
            .try_collect()
            .await?;
        Ok(documents)
    }

    async fn count_total_records<T, Q, O>(
        &self,
        collection_name: &str,
        query: Q,
        options: O,
    ) -> TuxedoResult<u64>
    where
        T: Send + Sync,
        Q: Into<Option<Document>>,
        O: Into<Option<CountOptions>>,
    {
        let total_documents = self
            .db
            .collection::<T>(collection_name)
            .count_documents(query)
            .with_options(options)
            .await?;
        Ok(total_documents)
    }
}
