use crate::replication::task::QueryConfig;
use crate::TuxedoResult;
use bson::Document;
use futures_util::TryStreamExt;
use mongodb::{Database, IndexModel};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug)]
pub(crate) struct DatabasePair {
    source: Database,
    target: Database,
}

impl DatabasePair {
    pub(crate) fn new(source: Database, target: Database) -> Self {
        Self { source, target }
    }

    pub(crate) async fn read<T: Serialize + DeserializeOwned + Send + Sync>(
        &self,
        collection_name: &str,
        config: &QueryConfig,
    ) -> TuxedoResult<Vec<T>> {
        Ok(self
            .source
            .collection::<T>(collection_name)
            .find(config.query.clone())
            .with_options(config.mongo_find_options())
            .await?
            .try_collect()
            .await?)
    }

    pub(crate) async fn read_documents(
        &self,
        collection_name: &str,
        config: &QueryConfig,
    ) -> TuxedoResult<Vec<Document>> {
        Ok(self
            .source
            .collection::<Document>(collection_name)
            .find(config.query.clone())
            .with_options(config.mongo_find_options())
            .await?
            .try_collect()
            .await?)
    }

    pub(crate) async fn read_total_documents<T: Send + Sync>(
        &self,
        collection_name: &str,
        query: Document,
    ) -> TuxedoResult<usize> {
        let total_documents = self
            .source
            .collection::<T>(collection_name)
            .count_documents(query)
            .await? as usize;
        Ok(total_documents)
    }

    pub(crate) async fn write<T: Send + Sync + Serialize>(
        &self,
        collection_name: &str,
        records: &Vec<T>,
    ) -> TuxedoResult<()> {
        self.target
            .collection::<T>(collection_name)
            .insert_many(records)
            .await?;
        Ok(())
    }

    // Indexes

    /// Copies the indexes from the source collection to the equivilant target collection
    pub(crate) async fn copy_indexes(&self, collection_name: &str) -> TuxedoResult<()> {
        let mut source_index_cursor = self
            .source
            .collection::<Document>(collection_name)
            .list_indexes()
            .await?;

        let mut indexes: Vec<IndexModel> = Vec::new();
        while let Some(index) = source_index_cursor.try_next().await? {
            // Skip the _id index as it's created automatically
            if index.keys.get("_id").is_some() {
                continue;
            }

            indexes.push(index);
        }

        self.target
            .collection::<Document>(collection_name)
            .create_indexes(indexes)
            .await?;
        Ok(())
    }

    // Database Initialization (testing) functions

    pub(crate) async fn clear_target_collections(&self) -> TuxedoResult<()> {
        let collections = self.target.list_collection_names().await?;

        println!("******************************");
        for collection_name in collections.into_iter() {
            println!("Dropping collection: {}", collection_name);
            self.target
                .collection::<mongodb::bson::Document>(&collection_name)
                .drop()
                .await?;
        }
        println!("******************************");
        println!("All collections have been dropped from target database.\n\n");
        Ok(())
    }

    pub(crate) async fn test_database_collection_source(&self) -> TuxedoResult<()> {
        self.test_database_connection(&self.source).await
    }

    pub(crate) async fn test_database_collection_target(&self) -> TuxedoResult<()> {
        self.test_database_connection(&self.target).await
    }

    async fn test_database_connection(&self, db: &Database) -> TuxedoResult<()> {
        db.list_collection_names()
            .await
            .expect("Failed to list connections for DB");
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    Clone,
    Mask,
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
