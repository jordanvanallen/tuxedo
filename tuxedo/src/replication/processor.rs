use super::{
    manager::ReplicationConfig,
    task::{ModelTask, QueryConfig, ReplicatorTask, Task},
    types::DatabasePair,
};
use crate::Mask;
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use mongodb_model::MongoModel;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

#[async_trait]
#[async_trait]
pub(crate) trait Processor: Send + Sync {
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        semaphore: Arc<Semaphore>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    );
}

pub(crate) struct ModelProcessor<T: MongoModel + Mask> {
    config: ProcessorConfig,
    _phantom_data: PhantomData<T>,
}

impl<T: MongoModel + Mask> ModelProcessor<T> {
    pub(crate) fn new(config: ProcessorConfig) -> Self {
        Self {
            config,
            _phantom_data: PhantomData,
        }
    }
}

pub(crate) struct ReplicatorProcessor<T: Send + Sync> {
    config: ProcessorConfig,
    collection_name: String,
    _phantom_data: PhantomData<T>,
}

impl<T: Send + Sync> ReplicatorProcessor<T> {
    pub(crate) fn new(config: ProcessorConfig, collection_name: String) -> Self {
        Self {
            config,
            collection_name,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T: MongoModel + Mask + 'static> Processor for ModelProcessor<T> {
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        semaphore: Arc<Semaphore>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);
        let total_documents: usize = match dbs
            .read_total_documents::<T>(T::COLLECTION_NAME, self.config.query.clone())
            .await
        {
            Ok(num_docs) => num_docs,
            Err(e) => {
                println!(
                    "Could not get total number of documents for collection: `{}`. Collection will be skipped. Encountered error: {e}",
                    T::COLLECTION_NAME,
                );
                return;
            }
        };

        let progress_bar = Arc::new(progress_bar);
        progress_bar.set_length(total_documents as u64);
        progress_bar.set_message(format!(
            "{} ({})",
            T::COLLECTION_NAME,
            std::any::type_name::<T>()
                .split("::")
                .last()
                .expect("Expected to get model name for progress bar")
        ));

        if let Err(e) = dbs.copy_indexes(T::COLLECTION_NAME).await {
            println!(
                "Error when copying indexes for collection `{}` from source to target - Error: {:?}",
                T::COLLECTION_NAME,
                e
            )
        }

        let batch_count = (total_documents + batch_size - 1) / batch_size;
        let strategy = default_config.strategy;

        for batch_index in 0..batch_count {
            let skip = batch_index * batch_size;
            let remaining_documents = total_documents.saturating_sub(skip);
            let limit = batch_size.min(remaining_documents);

            // This should never happen in theory
            if limit == 0 {
                // No more documents to process
                break;
            }

            let dbs = Arc::clone(&dbs);
            let query = self.config.query.clone();
            let strategy = strategy.clone();
            let progress_bar = Arc::clone(&progress_bar);

            let task = Box::new(ModelTask::<T>::new(
                dbs,
                QueryConfig::new(query, skip, limit, batch_size),
                strategy,
                progress_bar,
            ));

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("Expected OwnedSemaphorePermit");
            if task_sender.send(task).await.is_err() {
                // Channel closed, stop sending tasks
                break;
            }
            drop(permit);
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Processor for ReplicatorProcessor<T> {
    async fn run(
        &self,
        dbs: Arc<DatabasePair>,
        semaphore: Arc<Semaphore>,
        task_sender: mpsc::Sender<Box<dyn Task>>,
        default_config: ReplicationConfig,
        progress_bar: ProgressBar,
    ) {
        let batch_size = self.config.batch_size.unwrap_or(default_config.batch_size);
        let total_documents: usize = match dbs
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
        let progress_bar = Arc::new(progress_bar);
        progress_bar.set_length(total_documents as u64);
        progress_bar.set_message(format!(
            "{} ({})",
            &self.collection_name,
            std::any::type_name::<T>()
                .split("::")
                .last()
                .expect("Expected to get model name for progress bar")
        ));

        if let Err(e) = dbs.copy_indexes(&self.collection_name).await {
            println!(
                "Error when copying indexes for collection `{}` from source to target - Error: {:?}",
                &self.collection_name,
                e
            )
        }

        let batch_count = (total_documents + batch_size - 1) / batch_size;

        for batch_index in 0..batch_count {
            let skip = batch_index * batch_size;
            let remaining_documents = total_documents.saturating_sub(skip);
            let limit = batch_size.min(remaining_documents);

            // This should never happen in theory
            if limit == 0 {
                // No more documents to process
                break;
            }

            let dbs = Arc::clone(&dbs);
            let query = self.config.query.clone();
            let progress_bar = Arc::clone(&progress_bar);

            let task = Box::new(ReplicatorTask::<T>::new(
                dbs,
                self.collection_name.clone(),
                QueryConfig::new(query, skip, limit, batch_size),
                progress_bar,
            ));

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("Expected OwnedSemaphorePermit");
            if task_sender.send(task).await.is_err() {
                // Channel closed, stop sending tasks
                break;
            }
            drop(permit);
        }
    }
}

pub struct ProcessorConfig {
    batch_size: Option<usize>,
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

    pub fn batch_size<S: Into<Option<usize>>>(mut self, size: S) -> Self {
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
