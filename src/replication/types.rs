use crate::{TuxedoError, TuxedoResult};
use bson::{doc, Document};
use futures_util::TryStreamExt;
use mongodb::options::{FindOptions, InsertManyOptions};
use mongodb::Cursor;
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

    pub(crate) async fn read<T: Serialize + DeserializeOwned + Unpin + Send + Sync>(
        &self,
        collection_name: &str,
        query: Document,
        options: Option<FindOptions>,
    ) -> TuxedoResult<Cursor<T>> {
        Ok(self
            .source
            .collection::<T>(collection_name)
            .find(query)
            .with_options(options)
            .await?)
    }

    pub(crate) async fn get_average_document_size(
        &self,
        collection_name: &str,
    ) -> TuxedoResult<u64> {
        let stats = self
            .source
            .run_command(doc! { "collStats": collection_name })
            .await?;

        let avg_doc_size = stats.get_f64("avgObjSize").unwrap_or(1024.0);
        Ok(avg_doc_size as u64)
    }

    pub(crate) async fn read_documents(
        &self,
        collection_name: &str,
        query: Document,
        options: Option<FindOptions>,
    ) -> TuxedoResult<Cursor<Document>> {
        Ok(self
            .source
            .collection::<Document>(collection_name)
            .find(query)
            .with_options(options)
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
        records: &[T],
        options: Option<InsertManyOptions>,
    ) -> TuxedoResult<()> {
        self.target
            .collection::<T>(collection_name)
            .insert_many(records)
            .with_options(options)
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

        if indexes.is_empty() {
            println!("  -> No indexes to copy for '{}'", collection_name);
            return Ok(());
        }

        self.target
            .collection::<Document>(collection_name)
            .create_indexes(indexes)
            .await?;

        Ok(())
    }

    // Database Initialization (testing) functions

    pub(crate) async fn clear_target_collections(
        &self,
        collection_names: &[String],
    ) -> TuxedoResult<()> {
        let target_collections = self.target.list_collection_names().await?;

        println!("******************************");
        for collection_name in target_collections.into_iter() {
            // Skip system collections:
            // 1. Collections with system.* prefix
            // 2. Collections in admin database
            // 3. Collections in config database
            // 4. Special system collections
            if collection_name.starts_with("system.")
                || collection_name.starts_with("admin.")
                || collection_name.starts_with("config.")
            {
                println!("Skipping system collection: {}", collection_name);
                continue;
            }

            // Only drop collections that are in our list (collections + views)
            if collection_names.contains(&collection_name) {
                println!("Dropping collection/view: {}", collection_name);
                self.target
                    .collection::<mongodb::bson::Document>(&collection_name)
                    .drop()
                    .await?;
            } else {
                println!("Skipping collection not in drop list: {}", collection_name);
            }
        }
        println!("******************************");
        println!("Target database collections and views have been selectively dropped.\n\n");
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

    // Views

    /// Gets the names of all views in the source database
    pub(crate) async fn get_source_view_names(&self) -> TuxedoResult<Vec<String>> {
        let views = self.list_source_views().await?;
        Ok(views.into_iter().map(|view| view.name).collect())
    }

    /// Lists all views in the source database for copying
    pub(crate) async fn list_source_views(
        &self,
    ) -> TuxedoResult<Vec<mongodb::results::CollectionSpecification>> {
        let filter = doc! { "type": "view" };
        let mut cursor = self.source.list_collections().filter(filter).await?;

        let mut views = Vec::new();
        while let Some(collection_spec) = cursor.try_next().await? {
            // Verify this is actually a view with the required fields
            if collection_spec.options.view_on.is_some()
                && collection_spec.options.pipeline.is_some()
            {
                views.push(collection_spec);
            } else {
                println!(
                    "Warning: Collection '{}' marked as view but missing view fields",
                    collection_spec.name
                );
            }
        }

        if !views.is_empty() {
            println!("Found {} views to copy", views.len());
        }

        Ok(views)
    }

    /// Copies a single view from source to target (used by manager tasks)
    /// Note: Target views are cleared at startup, so no existence check needed
    pub(crate) async fn copy_single_view(
        &self,
        view_spec: &mongodb::results::CollectionSpecification,
    ) -> TuxedoResult<()> {
        // Extract view information from the CollectionSpecification
        let view_on = view_spec.options.view_on.as_ref().ok_or_else(|| {
            TuxedoError::Generic(format!("View '{}' missing viewOn", view_spec.name))
        })?;

        let pipeline = view_spec.options.pipeline.as_ref().ok_or_else(|| {
            TuxedoError::Generic(format!("View '{}' missing pipeline", view_spec.name))
        })?;

        // Create the view using the createView command
        let create_view_command = doc! {
            "create": &view_spec.name,
            "viewOn": view_on,
            "pipeline": pipeline,
        };

        self.target.run_command(create_view_command).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
