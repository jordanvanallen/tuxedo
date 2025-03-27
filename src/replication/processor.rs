use super::{
    manager::ReplicationConfig,
    task::{ModelTask, Task},
};
use crate::database::index::IndexManager;
use crate::database::pagination::PaginationOptions;
use crate::database::traits::{Destination, Source};
use crate::database::DatabasePair;
use crate::Mask;
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait]
pub(crate) trait Processor<S, D>
where
    Self: Send + Sync,
    S: Source,
    D: Destination,
{
    async fn run(
        &self,
        dbs: Arc<DatabasePair<S, D>>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    );

    fn entity_name(&self) -> &str;
}

pub(crate) struct ModelProcessor<T: Mask + Serialize + DeserializeOwned + Send + Sync> {
    config: ProcessorConfig,
    // TODO: This should become a generic name like entity_name
    entity_name: String,
    _phantom_data: PhantomData<T>,
}

impl<T: Mask + Serialize + DeserializeOwned + Send + Sync> ModelProcessor<T> {
    pub(crate) fn new(collection_name: impl Into<String>, config: ProcessorConfig) -> Self {
        Self {
            config,
            entity_name: collection_name.into(),
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T, S, D> Processor<S, D> for ModelProcessor<T>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Source,
    D: Destination,
{
    async fn run(
        &self,
        dbs: Arc<DatabasePair<S, D>>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);

        let total_records = match dbs
            .source
            .count_total_records::<T>(
                &self.entity_name,
                self.config.query.clone(),
            )
            .await
        {
            Ok(num_docs) => num_docs,
            Err(e) => {
                println!(
                    "Could not get total number of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
                    &self.entity_name,
                );
                return;
            }
        };

        let progress_bar = Arc::new(progress_bar);
        progress_bar.set_length(total_records);
        progress_bar.set_message(format!(
            "{} ({})",
            &self.entity_name,
            std::any::type_name::<T>()
                .split("::")
                .last()
                .expect("Expected to get model name for progress bar")
        ));

        if total_records == 0 {
            progress_bar.finish_with_message("No records to process.");
            println!("No records to process for entity: {}. Skipping.", &self.entity_name);
            return;
        }

        if let Err(e) = IndexManager::copy_indexes(&dbs, &self.entity_name).await {
            println!(
                "Error when copying indexes for entity `{}` from source to destination - Error: {:?}",
                &self.entity_name,
                e
            )
        }

        let batch_count = total_records.div_ceil(batch_size);
        let strategy = default_config.strategy;

        for batch_index in 0..batch_count {
            let start_position = batch_index * batch_size;
            let remaining_records = total_records.saturating_sub(start_position);
            let limit = batch_size.min(remaining_records);

            // This should never happen in theory
            if limit == 0 {
                // No more documents to process
                break;
            }

            let dbs = Arc::clone(&dbs);
            let query = self.config.query.clone();
            let strategy = strategy.clone();
            let progress_bar = Arc::clone(&progress_bar);

            let task = Box::new(ModelTask::<T, S, D>::new(
                dbs,
                self.entity_name.clone(),
                query,
                PaginationOptions { start_position, limit },
                strategy,
                progress_bar,
            ));

            if task_sender.send(task).await.is_err() {
                println!(
                    "Failed to send task to worker pool for entity '{}' (batch {}/{}). Channel closed, stopping processor.",
                    &self.entity_name,
                    batch_index + 1,
                    batch_count
                );
                // Channel closed, stop sending tasks
                break;
            }
        }
    }

    fn entity_name(&self) -> &str {
        &self.entity_name
    }
}

pub struct ProcessorConfig {
    batch_size: Option<u64>,
    query: Document,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        ProcessorConfigBuilder::new().build()
    }
}

pub struct ProcessorConfigBuilder {
    config: ProcessorConfig,
}

impl Default for ProcessorConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessorConfigBuilder {
    pub fn new() -> Self {
        let config = ProcessorConfig {
            batch_size: None,
            query: Document::new(),
        };
        Self { config }
    }

    pub fn batch_size<S: Into<Option<u64>>>(mut self, size: S) -> Self {
        self.config.batch_size = size.into();
        self
    }

    pub fn query<Q: Into<Document>>(mut self, query: Q) -> Self {
        self.config.query = query.into();
        self
    }

    pub fn build(self) -> ProcessorConfig {
        self.config
    }
}

// pub(crate) struct ReplicatorProcessor<T: Send + Sync> {
//     config: ProcessorConfig,
//     collection_name: String,
//     _phantom_data: PhantomData<T>,
// }

// impl<T: Send + Sync> ReplicatorProcessor<T> {
//     pub(crate) fn new(config: ProcessorConfig, collection_name: String) -> Self {
//         Self {
//             config,
//             collection_name,
//             _phantom_data: PhantomData,
//         }
//     }
// }
// #[async_trait]
// impl<T, S, D> Processor<S, D> for ReplicatorProcessor<T>
// where
//     T: Send + Sync + 'static,
//     S: Source,
//     D: Destination,
// {
//     async fn run(
//         &self,
//         dbs: Arc<DatabasePair<S, D>>,
//         task_sender: mpsc::Sender<Box<dyn Task>>,
//         default_config: ReplicationConfig,
//         progress_bar: ProgressBar,
//     ) {
//         let batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);
//         let total_documents = match dbs
//             .source
//             .count_total_records::<T>(
//                 &self.collection_name,
//                 self.config.query.clone(),
//                 None,
//             )
//             .await
//         {
//             Ok(num_docs) => num_docs,
//             Err(e) => {
//                 println!(
//                     "Could not get total number of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
//                     &self.collection_name,
//                 );
//                 return;
//             }
//         };
//         let progress_bar = Arc::new(progress_bar);
//         progress_bar.set_length(total_documents as u64);
//         progress_bar.set_message(format!(
//             "{} ({})",
//             &self.collection_name,
//             std::any::type_name::<T>()
//                 .split("::")
//                 .last()
//                 .expect("Expected to get model name for progress bar")
//         ));
//
//         if let Err(e) = dbs.copy_indexes(&self.collection_name).await {
//             println!(
//                 "Error when copying indexes for collection `{}` from source to target - Error: {:?}",
//                 &self.collection_name,
//                 e
//             )
//         }
//
//         let batch_count = total_documents.div_ceil(batch_size);
//
//         for batch_index in 0..batch_count {
//             let skip = batch_index * batch_size;
//             let remaining_documents = total_documents.saturating_sub(skip);
//             let limit = batch_size.min(remaining_documents);
//
//             // This should never happen in theory
//             if limit == 0 {
//                 // No more documents to process
//                 break;
//             }
//
//             let dbs = Arc::clone(&dbs);
//             let query = self.config.query.clone();
//             let progress_bar = Arc::clone(&progress_bar);
//
//             let task = Box::new(ReplicatorTask::<T, S, D>::new(
//                 dbs,
//                 self.collection_name.clone(),
//                 QueryConfig::new(query, skip, limit, batch_size),
//                 progress_bar,
//             ));
//
//             if task_sender.send(task).await.is_err() {
//                 println!(
//                     "Failed to send task to worker pool for collection '{}' (batch {}/{}). Channel closed, stopping processor.",
//                     &self.collection_name,
//                     batch_index + 1,
//                     batch_count
//                 );
//                 // Channel closed, stop sending tasks
//                 break;
//             }
//         }
//     }
//
//     fn collection_name(&self) -> &str {
//         &self.collection_name
//     }
// }

