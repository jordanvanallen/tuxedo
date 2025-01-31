use super::manager::{ReplicationConfig, ReplicationManager};
use super::processor::{Processor, ProcessorConfig};
use crate::replication::processor::{ModelProcessor, ReplicatorProcessor};
use crate::replication::types::{DatabasePair, ReplicationStrategy};
use crate::{Mask, TuxedoError, TuxedoResult};
use bson::Document;
use mongodb::{
    options::{ClientOptions, ReadConcern},
    Client,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ReplicationManagerBuilder {
    source_uri: Option<String>,
    target_uri: Option<String>,
    source_db: Option<String>,
    target_db: Option<String>,
    thread_count: usize,
    config: ReplicationConfig,
    processors: Vec<Box<dyn Processor>>,
}

impl Default for ReplicationManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationManagerBuilder {
    pub fn new() -> Self {
        // Default to all available threads
        let available_threads = num_cpus::get();

        Self {
            source_uri: None,
            target_uri: None,
            source_db: None,
            target_db: None,
            thread_count: available_threads,
            config: ReplicationConfig::default(),
            processors: Vec::new(),
        }
    }

    pub fn source_uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.source_uri = Some(uri.into());
        self
    }

    pub fn target_uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.target_uri = Some(uri.into());
        self
    }

    pub fn source_db<S: Into<String>>(mut self, db_name: S) -> Self {
        self.source_db = Some(db_name.into());
        self
    }

    pub fn target_db<S: Into<String>>(mut self, db_name: S) -> Self {
        self.target_db = Some(db_name.into());
        self
    }

    pub fn thread_count<S: Into<usize>>(mut self, count: S) -> Self {
        self.thread_count = count.into();
        self
    }

    // ******
    // Config
    // ******

    pub fn strategy<S: Into<ReplicationStrategy>>(mut self, strategy: S) -> Self {
        self.config.strategy = strategy.into();
        self
    }

    pub fn batch_size<S: Into<usize>>(mut self, size: S) -> Self {
        self.config.batch_size = size.into();
        self
    }

    pub fn bypass_document_validation<B: Into<bool>>(mut self, bypass: B) -> Self {
        self.config.bypass_document_validation = bypass.into();
        self
    }

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

    pub fn add_replicator(self, collection_name: impl Into<String>) -> Self {
        let config = ProcessorConfig::default();
        self.add_replicator_with_config(collection_name.into(), config)
    }

    pub fn add_replicator_with_config(
        mut self,
        collection_name: impl Into<String>,
        config: ProcessorConfig,
    ) -> Self {
        self.processors
            .push(Box::new(ReplicatorProcessor::<Document>::new(
                config,
                collection_name.into(),
            )));
        self
    }

    pub async fn build(self) -> TuxedoResult<ReplicationManager> {
        let source_uri = self
            .source_uri
            .ok_or(TuxedoError::ConfigError("No source_uri provided.".into()))?;
        let target_uri = self
            .target_uri
            .ok_or(TuxedoError::ConfigError("No target_uri provided.".into()))?;

        let max_pool_size = self.config.thread_count as u32;
        let min_pool_size = self.config.thread_count as u32;

        let mut source_client_options = ClientOptions::parse(source_uri).await?;
        source_client_options.max_pool_size = max_pool_size.into();
        source_client_options.min_pool_size = min_pool_size.into();
        // Make the database Read only as much as we have control to do
        source_client_options.read_concern = ReadConcern::majority().into();
        let source_client = Client::with_options(source_client_options)?;

        let mut target_client_options = ClientOptions::parse(target_uri).await?;
        target_client_options.max_pool_size = max_pool_size.into();
        target_client_options.min_pool_size = min_pool_size.into();
        let target_client = Client::with_options(target_client_options)?;

        let source_db_name = self
            .source_db
            .ok_or(TuxedoError::ConfigError("No source_db provided.".into()))?;
        let target_db_name = self
            .target_db
            .ok_or(TuxedoError::ConfigError("No target_db provided.".into()))?;

        // Ensure our database connections are actually valid and we can make the connection
        // We intentionally want to blow up here if we can't connect to *either* DB to avoid a giant mess
        let dbs = Arc::new(DatabasePair::new(
            source_client.database(&source_db_name),
            target_client.database(&target_db_name),
        ));
        dbs.test_database_collection_source()
            .await
            .expect("Could not create test connection to source database");
        dbs.test_database_collection_target()
            .await
            .expect("Could not create test connection to target database");

        println!("Dropping collections from target database before beginning...");
        // Collect collection names from processors
        let collection_names: Vec<String> = self.processors.iter()
            .map(|p| p.collection_name().to_string())
            .collect();
        dbs.clear_target_collections(&collection_names).await.expect(
            "Expected to successfully drop target database collections before replication",
        );

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
