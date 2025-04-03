use super::manager::{ReplicationConfig, ReplicationManager};
use super::processor::{Processor, ProcessorConfig, ProcessorFactory};
use super::types::{ReplicationStrategy, StreamingMode};
use crate::database::{traits::{Destination, Source}, DatabasePair};
use crate::{Mask, TuxedoError, TuxedoResult};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ReplicationManagerBuilder<S, D>
where
    S: Source + 'static,
    D: Destination + 'static,
{
    source: Option<S>,
    destination: Option<D>,
    thread_count: usize,
    config: ReplicationConfig,
    processors: Vec<Box<dyn Processor<S, D>>>,
    processor_factory: Option<ProcessorFactory<S>>,
}

impl<S, D> Default for ReplicationManagerBuilder<S, D>
where
    S: Source + 'static,
    D: Destination + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, D> ReplicationManagerBuilder<S, D>
where
    S: Source + 'static,
    D: Destination + 'static,
{
    pub fn new() -> Self {
        // Default to all available threads
        let available_threads = num_cpus::get();

        Self {
            source: None,
            destination: None,
            thread_count: available_threads,
            config: ReplicationConfig::default(),
            processors: Vec::new(),
            processor_factory: None,
        }
    }

    pub fn source(mut self, source: S) -> Self {
        self.source = Some(source);
        self.processor_factory = Some(ProcessorFactory::new());
        self
    }

    pub fn destination(mut self, destination: D) -> Self {
        self.destination = Some(destination);
        self
    }

    // ******
    // Config
    // ******

    pub fn thread_count(mut self, count: impl Into<usize>) -> Self {
        self.thread_count = count.into();
        self
    }

    pub fn strategy(mut self, strategy: impl Into<ReplicationStrategy>) -> Self {
        self.config.strategy = strategy.into();
        self
    }

    pub fn batch_size(mut self, size: impl Into<u64>) -> Self {
        self.config.batch_size = size.into();
        self
    }

    /// Set the streaming mode for document processing
    ///
    /// - StreamingMode::Batch (default): Processes documents in batches
    ///   Best for small to medium collections with small documents
    ///
    /// - StreamingMode::Streaming: Streams documents to reduce memory usage
    ///   Best for very large collections or documents to reduce memory pressure
    pub fn streaming_mode(mut self, mode: StreamingMode) -> Self {
        self.config.streaming_mode = mode;
        self
    }

    /// Add a processor for entity type T with default settings (all items)
    pub fn add_processor<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static>(
        mut self,
        entity_name: impl Into<String>,
    ) -> Self
    where
        S::Query: Default,
    {
        let factory = self.processor_factory.as_ref()
            .expect("Cannot add processor before setting source");

        let config = ProcessorConfig::<S::Query>::default();
        let processor = factory.create_model_processor::<T>(entity_name, config);

        self.processors.push(Box::new(processor));
        self
    }

    /// Add a processor with a custom configuration
    pub fn add_processor_with_config<T>(
        mut self,
        entity_name: impl Into<String>,
        config: ProcessorConfig<S::Query>,
    ) -> Self
    where
        T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
        S::Query: Default,
    {
        let factory = self
            .processor_factory
            .as_ref()
            .expect("Cannot add processor before setting source");

        let processor = factory.create_model_processor::<T>(entity_name, config);

        self.processors.push(Box::new(processor));
        self
    }

    pub async fn build(self) -> TuxedoResult<ReplicationManager<S, D>> {
        let dbs = Arc::new(DatabasePair::new(
            self.source.ok_or(TuxedoError::ConfigError("No source database provided".into()))?,
            self.destination.ok_or(TuxedoError::ConfigError("No destination database provided".into()))?,
        ));

        // Ensure our database connections are actually valid, and we can make the connection
        // We intentionally want to panic here if we can't connect to *either* DB to avoid a giant mess
        dbs.source
            .test_database_connection()
            .await
            .expect("Could not create test connection to source database");
        dbs.destination
            .test_database_connection()
            .await
            .expect("Could not create test connection to destination database");

        println!("Dropping tables/collections from destination database before beginning...");
        // Collect collection names from processors
        let collection_names: Vec<String> = self.processors.iter()
            .map(|p| p.entity_name().to_string())
            .collect();
        dbs.destination
            .clear_database(&collection_names)
            .await
            .expect("Expected to successfully drop destination database tables/collections before replication");

        let (task_sender, task_receiver) = mpsc::channel(self.config.thread_count * 4);

        let task_manager = ReplicationManager {
            dbs,
            processors: self.processors,
            config: self.config,
            task_receiver,
            task_sender,
        };

        Ok(task_manager)
    }
}
