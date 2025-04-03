use crate::database::{
    index::{IndexField, SourceIndexes},
    mongodb::source_builder::MongodbSourceBuilder,
    pagination::PaginationOptions,
    traits::{ConnectionTestable, ReadOperations, Source, SourceIndexManager},
};
use crate::TuxedoResult;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::{bson::Document, options::{CountOptions, FindOptions}, Client, Database, IndexModel};
use serde::de::DeserializeOwned;
use std::collections::HashMap;

pub struct MongodbSource {
    client: Client,
    db: Database,
    read_options: FindOptions,
    count_options: Option<CountOptions>,
}

impl MongodbSource {
    pub fn builder() -> MongodbSourceBuilder {
        MongodbSourceBuilder::new()
    }

    pub(crate) async fn new(
        client: Client,
        db: Database,
        read_options: FindOptions,
        count_options: Option<CountOptions>,
    ) -> TuxedoResult<Self>
    {
        Ok(Self {
            client,
            db,
            read_options,
            count_options,
        })
    }

    pub(crate) fn generate_default_index_name(&self, collection_name: &str, fields: &[IndexField]) -> String {
        let field_names: String = fields
            .iter()
            .map(|field| {
                format!("{}_{}", field.name, field.direction)
            })
            .collect::<Vec<String>>()
            .join("_");

        format!("idx_{}_{}", collection_name, field_names)
    }
}

#[async_trait]
impl Source for MongodbSource {
    async fn prepare_database(&self) -> TuxedoResult<()> {
        self.client.warm_connection_pool().await;
        Ok(())
    }
}

#[async_trait]
impl ConnectionTestable for MongodbSource {
    async fn test_database_connection(&self) -> TuxedoResult<()> {
        self.db.list_collection_names().await?;
        Ok(())
    }
}

#[async_trait]
impl SourceIndexManager for MongodbSource {
    async fn list_indexes(&self, collection_name: &str) -> TuxedoResult<SourceIndexes> {
        let source_indexes: Vec<IndexModel> = self
            .db
            .collection::<Document>(collection_name)
            .list_indexes()
            .await?
            .try_collect()
            .await?;

        // Grab all indexes except the _id index (created by default by MongoDB)
        let filtered_models: Vec<IndexModel> = source_indexes
            .into_iter()
            .filter(|index| index.keys.get("_id").is_none())
            .collect();

        // Convert to SourceIndexes using the From implementation
        let mut source_indexes = SourceIndexes::from((filtered_models, collection_name.to_string()));
        
        // Replace any unnamed indexes with generated names
        for index_config in &mut source_indexes.indexes {
            if index_config.name == "unnamed_index" {
                index_config.name = self.generate_default_index_name(collection_name, &index_config.fields);
            }
        }

        Ok(source_indexes)
    }
}

#[async_trait]
impl ReadOperations for MongodbSource {
    type Query = Document;
    type ReadOptions = FindOptions;
    type RecordCountOptions = CountOptions;

    fn build_chunk_read_options(&self, options: &PaginationOptions) -> Self::ReadOptions {
        let mut read_options = self.read_options.clone();
        read_options.skip = options.start_position.into();
        read_options.limit = Some(options.limit as i64);
        read_options
    }

    async fn read_chunk<T>(
        &self,
        collection_name: &str,
        query: Self::Query,
        pagination_options: PaginationOptions,
    ) -> TuxedoResult<Vec<T>>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let read_options = self.build_chunk_read_options(&pagination_options);

        let documents = self
            .db
            .collection::<T>(collection_name)
            .find(query)
            .with_options(read_options)
            .await?
            .try_collect()
            .await?;
        Ok(documents)
    }

    async fn count_total_records<T>(
        &self,
        collection_name: &str,
        query: Self::Query,
    ) -> TuxedoResult<u64>
    where
        T: Send + Sync,
    {
        let total_documents = self
            .db
            .collection::<T>(collection_name)
            .count_documents(query)
            .with_options(self.count_options.clone())
            .await?;
        Ok(total_documents)
    }
}
