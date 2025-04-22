use super::{
    manager::ReplicationConfig,
    task::{ModelTask, ReplicatorTask, Task},
    types::DatabasePair,
};
use crate::replication::task::TaskConfig;
use crate::{Mask, TuxedoResult};
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait]
pub(crate) trait Processor: Send + Sync {
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    );

    async fn get_total_documents(&self, dbs: &Arc<DatabasePair>, query: Document) -> TuxedoResult<usize> {
        match dbs.read_total_documents::<Document>(self.collection_name(), query).await {
            Ok(total_documents) => Ok(total_documents),
            Err(e) => {
                println!(
                    "Could not get total number of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
                    self.collection_name(),
                );
                Err(e)
            }
        }
    }

    fn setup_progress_bar(
        &self,
        progress_bar: ProgressBar,
        total_documents: usize,
        entity_name: &str,
    ) -> Arc<ProgressBar> {
        let progress_bar = Arc::new(progress_bar);

        progress_bar.set_length(total_documents as u64);
        progress_bar.set_message(format!(
            "{} ({})",
            self.collection_name(),
            entity_name,
        ));

        progress_bar
    }

    async fn copy_indexes(&self, dbs: &Arc<DatabasePair>) {
        if let Err(e) = dbs.copy_indexes(self.collection_name()).await {
            println!(
                "Error when copying indexes for collection `{}` from source to target - Error: {:?}",
                self.collection_name(),
                e
            )
        }
    }

    fn collection_name(&self) -> &str;
}

pub(crate) struct ModelProcessor<T: Mask + Serialize + DeserializeOwned + Send + Sync> {
    config: ProcessorConfig,
    collection_name: String,
    _phantom_data: PhantomData<T>,
}

impl<T: Mask + Serialize + DeserializeOwned + Send + Sync> ModelProcessor<T> {
    pub(crate) fn new(collection_name: impl Into<String>, config: ProcessorConfig) -> Self {
        Self {
            config,
            collection_name: collection_name.into(),
            _phantom_data: PhantomData,
        }
    }
}

pub(crate) struct ReplicatorProcessor<T: Send + Sync> {
    config: ReplicatorConfig,
    collection_name: String,
    _phantom_data: PhantomData<T>,
}

impl<T: Send + Sync> ReplicatorProcessor<T> {
    pub(crate) fn new(config: ReplicatorConfig, collection_name: String) -> Self {
        Self {
            config,
            collection_name,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static> Processor
for ModelProcessor<T>
{
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let mut batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);
        let total_documents = match self.get_total_documents(
            &dbs,
            self.config.query.clone(),
        ).await {
            Ok(total_documents) => total_documents,
            Err(_) => return,
        };

        let progress_bar = self.setup_progress_bar(
            progress_bar,
            total_documents,
            std::any::type_name::<T>()
                .split("::")
                .last()
                .expect("Expected to get model name for progress bar"),
        );

        if total_documents == 0 {
            progress_bar.finish_with_message("No records to process.");
            println!(
                "No records to process for collection: {}. Skipping.",
                &self.collection_name,
            );
            return;
        }

        // TODO: Confirm this is the logic we want for adaptive batching
        if self.config.adaptive_batching == Some(true) || default_config.adaptive_batching {
            // TODO: Inject read options here for FindOptions
            let mut read_options = default_config.read_options.clone();
            read_options.skip = Some((total_documents - 20) as u64);
            read_options.limit = 20.into();
            let sample: Vec<T> = match dbs
                .read::<T>(
                    &self.collection_name,
                    self.config.query.clone(),
                    read_options.into(),
                )
                .await {
                Ok(records) => records,
                Err(e) => {
                    println!(
                        "Could not get sample of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
                        &self.collection_name,
                    );
                    Vec::new()
                }
            };

            if !sample.is_empty() {
                // Calculate the average document size for this collection
                let total_size: u64 = sample
                    .iter()
                    .map(|document| {
                        // Use serde_json to estimate size
                        match serde_json::to_string(document) {
                            Ok(json) => json.len(),
                            Err(_) => 1024, // Fallback size if conversion fails
                        }
                    })
                    .sum::<usize>() as u64;

                let average_document_size: u64 = total_size / sample.len() as u64;

                // Target batch size in bytes - try to determine optimally if no explicit setting
                let target_bytes: u64 = if let Some(target) = self
                    .config
                    .target_batch_bytes
                    .or(default_config.target_batch_bytes)
                {
                    target as u64
                } else {
                    calculate_optimal_target_bytes(average_document_size)
                };

                // Calculate optimal documents per batch based on size
                let optimal_count = (target_bytes / average_document_size as u64).max(10);

                // Apply reasonable limits
                let adapted_batch_size = optimal_count.clamp(100, 10_000);

                batch_size = adapted_batch_size;
                // TODO: Set cursor batch size
            }
        }

        self.copy_indexes(&dbs).await;

        let batch_count = total_documents.div_ceil(batch_size as usize);
        let strategy = default_config.strategy;
        let write_options = default_config.write_options;

        for batch_index in 0..batch_count {
            let skip = batch_index * batch_size as usize;
            let remaining_documents = total_documents.saturating_sub(skip);
            let limit = batch_size.min(remaining_documents as u64) as i64;

            // This should never happen in theory
            if limit == 0 {
                // No more documents to process
                break;
            }

            let dbs = Arc::clone(&dbs);
            let query = self.config.query.clone();
            let strategy = strategy.clone();
            let progress_bar = Arc::clone(&progress_bar);

            let mut read_options = default_config.read_options.clone();
            read_options.skip = (skip as u64).into();
            read_options.limit = limit.into();

            let task = Box::new(ModelTask::<T>::new(
                dbs,
                self.collection_name.clone(),
                TaskConfig {
                    query,
                    read_options,
                    write_options: write_options.clone(),
                },
                strategy,
                progress_bar,
            ));

            if task_sender.send(task).await.is_err() {
                println!(
                    "Failed to send task to worker pool for collection '{}' (batch {}/{}). Channel closed, stopping processor.",
                    &self.collection_name,
                    batch_index + 1,
                    batch_count
                );
                // Channel closed, stop sending tasks
                break;
            }
        }
    }

    fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

fn calculate_optimal_target_bytes(average_document_size: u64) -> u64 {
    // For very small documents (<1KB), use larger batches to reduce i/o overhead
    if average_document_size < 1024 {
        return to_mb(12);
    }

    // For small documents (1KB-10KB), use standard batch size
    if average_document_size < 10 * 1024 {
        return to_mb(8);
    }

    // For medium documents (10KB-100KB), use moderate batch size
    if average_document_size < 100 * 1024 {
        return to_mb(4);
    }

    // For large documents (100KB-500KB), use smaller batches
    if average_document_size < 500 * 1024 {
        return to_mb(2);
    }

    // For very large documents (>500KB), use minimal batches
    // to avoid excessive memory pressure
    1024 * 1024
}

fn to_mb(size: u64) -> u64 {
    size * 1024 * 1024
}

#[async_trait]
impl<T: Send + Sync + 'static> Processor for ReplicatorProcessor<T> {
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);
        let total_documents = match dbs
            .read_total_documents::<T>(&self.collection_name, self.config.query.clone())
            .await
        {
            Ok(num_docs) => num_docs,
            Err(e) => {
                println!(
                    "Could not get total number of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
                    &self.collection_name,
                );
                return;
            }
        };
        let progress_bar = self.setup_progress_bar(
            progress_bar,
            total_documents,
            "Document",
        );

        if total_documents == 0 {
            progress_bar.finish_with_message("No records to process.");
            println!(
                "No records to process for collection: {}. Skipping.",
                &self.collection_name,
            );
            return;
        }

        self.copy_indexes(&dbs).await;

        let batch_count = total_documents.div_ceil(batch_size as usize);
        let write_options = default_config.write_options;

        for batch_index in 0..batch_count {
            let skip = batch_index * batch_size as usize;
            let remaining_documents = total_documents.saturating_sub(skip);
            let limit = batch_size.min(remaining_documents as u64) as i64;

            // This should never happen in theory
            if limit == 0 {
                // No more documents to process
                break;
            }

            let dbs = Arc::clone(&dbs);
            let query = self.config.query.clone();
            let progress_bar = Arc::clone(&progress_bar);

            let mut read_options = default_config.read_options.clone();
            read_options.skip = (skip as u64).into();
            read_options.limit = limit.into();

            let task = Box::new(ReplicatorTask::<T>::new(
                dbs,
                self.collection_name.clone(),
                TaskConfig {
                    query,
                    read_options,
                    write_options: write_options.clone(),
                },
                // QueryConfig::new(query, skip, limit, batch_size),
                self.config.lambda.clone(),
                progress_bar,
            ));

            if task_sender.send(task).await.is_err() {
                println!(
                    "Failed to send task to worker pool for collection '{}' (batch {}/{}). Channel closed, stopping processor.",
                    &self.collection_name,
                    batch_index + 1,
                    batch_count
                );
                // Channel closed, stop sending tasks
                break;
            }
        }
    }

    fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

#[derive(Debug, Default)]
pub struct ProcessorConfig {
    adaptive_batching: Option<bool>,
    target_batch_bytes: Option<usize>,
    batch_size: Option<u64>,
    query: Document,
}

#[derive(Debug, Default)]
pub struct ProcessorConfigBuilder {
    config: ProcessorConfig,
}

impl ProcessorConfig {
    pub fn builder() -> ProcessorConfigBuilder {
        ProcessorConfigBuilder::new()
    }
}

impl ProcessorConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn batch_size(mut self, size: impl Into<u64>) -> Self {
        self.config.batch_size = Some(size.into());
        self
    }

    pub fn target_batch_bytes(mut self, size: impl Into<Option<usize>>) -> Self {
        self.config.target_batch_bytes = size.into();
        self
    }

    pub fn query<Q: Into<Document>>(mut self, query: Q) -> Self {
        self.config.query = query.into();
        self
    }

    pub fn adaptive_batching(mut self, enabled: bool) -> Self {
        self.config.adaptive_batching = Some(enabled);
        self
    }

    pub fn build(self) -> ProcessorConfig {
        self.config
    }
}

#[derive(Default)]
pub struct ReplicatorConfig {
    batch_size: Option<u64>,
    target_batch_bytes: Option<u64>,
    query: Document,
    adaptive_batching: Option<bool>,
    lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
}

impl ReplicatorConfig {
    fn new(
        batch_size: Option<u64>,
        target_batch_bytes: Option<u64>,
        query: Document,
        adaptive_batching: Option<bool>,
        lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
    ) -> Self {
        Self {
            batch_size,
            target_batch_bytes,
            query,
            adaptive_batching,
            lambda,
        }
    }

    pub fn builder() -> ReplicationConfigBuilder {
        ReplicationConfigBuilder::new()
    }
}

#[derive(Default)]
pub struct ReplicationConfigBuilder {
    batch_size: Option<u64>,
    target_batch_bytes: Option<u64>,
    query: Document,
    adaptive_batching: Option<bool>,
    lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
}

impl ReplicationConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn batch_size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.batch_size = size.into();
        self
    }

    pub fn target_batch_bytes(mut self, size: impl Into<Option<u64>>) -> Self {
        self.target_batch_bytes = size.into();
        self
    }

    pub fn query(mut self, query: impl Into<Document>) -> Self {
        self.query = query.into();
        self
    }

    pub fn adaptive_batching(mut self, enabled: impl Into<bool>) -> Self {
        self.adaptive_batching = Some(enabled.into());
        self
    }

    pub fn mask<F>(mut self, lambda: F) -> Self
    where
        F: Fn(&mut Document) + Send + Sync + 'static,
    {
        self.lambda = Some(Arc::new(lambda));
        self
    }

    pub fn build(self) -> ReplicatorConfig {
        ReplicatorConfig::new(
            self.batch_size,
            self.target_batch_bytes,
            self.query,
            self.adaptive_batching,
            self.lambda,
        )
    }
}
