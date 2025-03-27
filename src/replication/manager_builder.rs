use super::manager::{ReplicationConfig, ReplicationManager};
use super::processor::{Processor, ProcessorConfig};
use crate::database::{traits::{Destination, Source}, DatabasePair};
use crate::replication::processor::ModelProcessor;
use crate::replication::types::ReplicationStrategy;
use crate::{Mask, TuxedoError, TuxedoResult};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ReplicationManagerBuilder<S, D>
where
    S: Source,
    D: Destination,
{
    source: Option<S>,
    destination: Option<D>,
    thread_count: usize,
    config: ReplicationConfig,
    processors: Vec<Box<dyn Processor<S, D>>>,
}

impl<S: Source, D: Destination> Default for ReplicationManagerBuilder<S, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Source, D: Destination> ReplicationManagerBuilder<S, D> {
    pub fn new() -> Self {
        // Default to all available threads
        let available_threads = num_cpus::get();

        Self {
            source: None,
            destination: None,
            thread_count: available_threads,
            config: ReplicationConfig::default(),
            processors: Vec::new(),
        }
    }

    pub fn source(mut self, source: S) -> Self {
        self.source = Some(source);
        self
    }

    pub fn destination(mut self, destination: D) -> Self {
        self.destination = Some(destination);
        self
    }

    // ******
    // Config
    // ******

    pub fn thread_count<C: Into<usize>>(mut self, count: C) -> Self {
        self.thread_count = count.into();
        self
    }

    pub fn strategy<X: Into<ReplicationStrategy>>(mut self, strategy: X) -> Self {
        self.config.strategy = strategy.into();
        self
    }

    pub fn batch_size<X: Into<u64>>(mut self, size: X) -> Self {
        self.config.batch_size = size.into();
        self
    }

    // pub fn bypass_document_validation<B: Into<bool>>(mut self, bypass: B) -> Self {
    //     self.config.bypass_document_validation = bypass.into();
    //     self
    // }

    pub fn add_processor<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static>(
        self,
        collection_name: impl Into<String>,
    ) -> Self {
        let config = ProcessorConfig::default();
        self.add_processor_with_config::<T>(collection_name, config)
    }

    pub fn add_processor_with_config<
        T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    >(
        mut self,
        collection_name: impl Into<String>,
        config: ProcessorConfig,
    ) -> Self {
        self.processors
            .push(Box::new(ModelProcessor::<T>::new(collection_name, config)));
        self
    }

    // pub fn add_replicator(self, collection_name: impl Into<String>) -> Self {
    //     let config = ProcessorConfig::default();
    //     self.add_replicator_with_config(collection_name.into(), config)
    // }
    //
    // pub fn add_replicator_with_config(
    //     mut self,
    //     collection_name: impl Into<String>,
    //     config: ProcessorConfig,
    // ) -> Self {
    //     self.processors
    //         .push(Box::new(ReplicatorProcessor::<Document>::new(
    //             config,
    //             collection_name.into(),
    //         )));
    //     self
    // }

    pub async fn build(self) -> TuxedoResult<ReplicationManager<S, D>> {
        let dbs = Arc::new(DatabasePair::new(
            self.source.ok_or(TuxedoError::ConfigError("No source database provided".into()))?,
            self.destination.ok_or(TuxedoError::ConfigError("No destination database provided".into()))?,
        ));

        // Ensure our database connections are actually valid and we can make the connection
        // We intentionally want to blow up here if we can't connect to *either* DB to avoid a giant mess
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

        let (task_sender, task_receiver) = mpsc::channel(self.config.thread_count);

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
