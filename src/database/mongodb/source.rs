use crate::database::{
    index::{IndexConfig, IndexDirection, IndexField, IndexType, SourceIndexes},
    mongodb::source_builder::MongodbSourceBuilder,
    pagination::PaginationOptions,
    traits::{ConnectionTestable, ReadOperations, Source, SourceIndexManager},
};
use crate::TuxedoResult;
use bson::Bson;
use futures_util::TryStreamExt;
use mongodb::{bson::Document, options::{CountOptions, FindOptions}, Client, Database, IndexModel};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use async_trait::async_trait;

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
    async fn list_indexes(&self, entity_name: &str) -> TuxedoResult<SourceIndexes> {
        let source_indexes: Vec<IndexModel> = self
            .db
            .collection::<Document>(entity_name)
            .list_indexes()
            .await?
            .try_collect()
            .await?;

        let indexes: Vec<IndexConfig> = source_indexes
            .into_iter()
            // Skip the _id index as it's created automatically
            .filter(|index| index.keys.get("_id").is_none())
            .map(|index| {
                let keys = &index.keys;

                let fields: Vec<IndexField> = keys.iter()
                    .map(|(name, value)| {
                        let direction = match value {
                            Bson::Int32(1) => IndexDirection::Ascending,
                            Bson::Int32(-1) => IndexDirection::Descending,
                            _ => IndexDirection::Ascending, // Default fallback
                        };

                        IndexField {
                            name: name.into(),
                            direction,
                        }
                    })
                    .collect();

                // Default to Standard if no options
                let mut index_type = IndexType::Standard;
                let mut options: HashMap<String, serde_json::Value> = HashMap::new();

                if let Some(opts) = &index.options {
                    // Determine index type
                    if opts.unique.is_some() {
                        index_type = IndexType::Unique;
                        options.insert("unique".into(), serde_json::Value::Bool(true));
                    } else if opts.text_index_version.is_some() {
                        index_type = IndexType::Text;
                    } else if opts.sphere_2d_index_version.is_some() {
                        index_type = IndexType::Geo2DSphere;
                    }

                    // Add sparse option if present
                    if let Some(sparse) = opts.sparse {
                        options.insert("sparse".into(), serde_json::Value::Bool(sparse));
                    }
                }

                let name = index.options
                    .as_ref()
                    .and_then(|opts| opts.name.clone())
                    .unwrap_or_else(|| {
                        self.generate_default_index_name(&entity_name, &fields)
                    });

                IndexConfig {
                    name,
                    fields,
                    index_type,
                    options,
                }
            })
            .collect();

        let source_index = SourceIndexes {
            entity_name: entity_name.into(),
            indexes,
        };

        Ok(source_index)
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
