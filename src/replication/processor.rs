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

pub(crate) struct ModelProcessor<T, S>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync,
    S: Source,
{
    config: ProcessorConfig<S::Query>,
    entity_name: String,
    _phantom_data: PhantomData<(T, S)>,
}

impl<T, S> ModelProcessor<T, S>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync,
    S: Source,
{
    pub(crate) fn new(entity_name: impl Into<String>, config: ProcessorConfig<S::Query>) -> Self {
        Self {
            config,
            entity_name: entity_name.into(),
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T, S, D> Processor<S, D> for ModelProcessor<T, S>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Source + 'static,
    D: Destination + 'static,
{
    async fn run(
        &self,
        dbs: Arc<DatabasePair<S, D>>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let batch_size = self.config.batch_size.unwrap_or(dbs.source.batch_size());

        let total_records = match dbs
            .source
            .count_total_records::<T>(&self.entity_name, self.config.query.clone())
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
            println!(
                "No records to process for entity: {}. Skipping.",
                &self.entity_name
            );
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
                PaginationOptions {
                    start_position,
                    limit,
                },
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

/// Configures how a processor will handle an entity replication
#[derive(Debug, Clone, Default)]
pub struct ProcessorConfig<Q> {
    /// Optional batch size, will use the default from ReplicationConfig if None
    pub(crate) batch_size: Option<u64>,
    /// Query to filter which records to replicate
    pub(crate) query: Q,
}

impl<Q> ProcessorConfig<Q> {
    /// Create a new ProcessorConfig with the given query and batch size
    ///
    /// Note: This is a convenience method for crate usage.
    /// External users should use the builder pattern.
    pub(crate) fn new(query: Q, batch_size: Option<u64>) -> Self {
        Self { query, batch_size }
    }

    /// Create a builder for ProcessorConfig
    pub fn builder() -> ProcessorConfigBuilder<Q>
    where
        Q: Default,
    {
        ProcessorConfigBuilder::default()
    }
}

/// Builder for ProcessorConfig to allow flexible configuration
#[derive(Default)]
pub struct ProcessorConfigBuilder<Q>
where
    Q: Default,
{
    config: ProcessorConfig<Q>,
}

impl<Q: Default> ProcessorConfigBuilder<Q> {
    /// Create a new ProcessorConfigBuilder with default query
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the query
    pub fn query(mut self, query: Q) -> Self {
        self.config.query = query;
        self
    }

    /// Set the batch size
    pub fn batch_size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.config.batch_size = size.into();
        self
    }

    /// Build the final ProcessorConfig
    pub fn build(self) -> ProcessorConfig<Q> {
        self.config
    }
}

/// Factory that knows the source type and creates processors for specific entity types
// This factory lets us prevent the user from passing in S to every #add_processor() function
// call to get S::Query to know the appropriate type for that database implementation
pub struct ProcessorFactory<S>
where
    S: Source + 'static,
{
    _phantom: PhantomData<S>,
}

impl<S> ProcessorFactory<S>
where
    S: Source + 'static,
{
    /// Create a new processor factory for the given source type
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    /// Create a model processor with the given configuration
    ///
    /// If options in the config are None, they will use default values from ReplicationConfig
    pub fn create_model_processor<T>(
        &self,
        entity_name: impl Into<String>,
        config: ProcessorConfig<S::Query>,
    ) -> ModelProcessor<T, S>
    where
        T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        ModelProcessor::new(entity_name, config)
    }

    // Get a processor config builder
    // pub fn config_builder(&self) -> ProcessorConfigBuilder<S::Query>
    // where
    //     S::Query: Default,
    // {
    //     ProcessorConfigBuilder::default()
    // }
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
