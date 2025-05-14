use super::manager::{ReplicationConfig, ReplicationManager};
use super::processor::{Processor, ProcessorConfig, ReplicatorConfig};
use crate::replication::processor::{ModelProcessor, ReplicatorProcessor};
use crate::replication::types::{DatabasePair, ReplicationStrategy};
use crate::{Mask, TuxedoError, TuxedoResult};
use bson::Document;
use mongodb::options::FindOptions;
use mongodb::{
    options::{ClientOptions, Compressor, InsertManyOptions, ReadConcern},
    Client,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use url::Url;

pub struct ReplicationManagerBuilder {
    source_uri: Option<String>,
    target_uri: Option<String>,
    source_db: Option<String>,
    target_db: Option<String>,
    thread_count: usize,
    config: ReplicationConfig,
    compressors: Option<Vec<Compressor>>,
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
            compressors: None,
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

    pub fn batch_size(mut self, size: impl Into<u64>) -> Self {
        self.config.batch_size = size.into();
        self
    }

    pub fn write_batch_size(mut self, size: impl Into<u64>) -> Self {
        self.config.write_batch_size = size.into();
        self
    }

    pub fn write_options(mut self, options: impl Into<InsertManyOptions>) -> Self {
        self.config.write_options = options.into();
        self
    }

    pub fn read_options(mut self, options: impl Into<FindOptions>) -> Self {
        self.config.read_options = options.into();
        self
    }

    pub fn add_compression(mut self) -> Self {
        self.compressors = Some(vec![
            // Zstd offers the best compression ratio and good performance
            Compressor::Zstd { level: None },
            // Zlib is widely supported with good compression
            Compressor::Zlib { level: None },
            // Snappy is fastest but has lower compression ratio
            Compressor::Snappy,
        ]);
        self
    }

    pub fn adaptive_batching(mut self) -> Self {
        self.config.adaptive_batching = true;
        self
    }

    pub fn optimize_for_performance(self, compression: bool) -> Self {
        let mut builder = self;

        builder.config.write_options.ordered = false.into();
        builder.config.write_options.bypass_document_validation = true.into();

        if compression {
            builder = builder.add_compression();
        }

        builder
    }

    pub fn add_processor<T: Mask + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static>(
        self,
        collection_name: impl Into<String>,
    ) -> Self {
        let config = ProcessorConfig::default();
        self.add_processor_with_config::<T>(collection_name, config)
    }

    pub fn add_processor_with_config<
        T: Mask + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
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
        let config = ReplicatorConfig::default();
        self.add_replicator_with_config(collection_name.into(), config)
    }

    pub fn add_replicator_with_config(
        mut self,
        collection_name: impl Into<String>,
        config: ReplicatorConfig,
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
            .clone()
            .ok_or(TuxedoError::ConfigError("No source_uri provided.".into()))?;
        let target_uri = self
            .target_uri
            .clone()
            .ok_or(TuxedoError::ConfigError("No target_uri provided.".into()))?;

        let compressors = self.compressors.clone();

        let max_pool_size = (self.config.thread_count * 2) as u32;
        let min_pool_size = self.config.thread_count as u32;
        let max_connecting = self.config.thread_count as u32;

        let mut source_client_options = ClientOptions::parse(&source_uri).await?;
        source_client_options.max_pool_size = max_pool_size.into();
        source_client_options.min_pool_size = min_pool_size.into();
        source_client_options.max_connecting = max_connecting.into();
        source_client_options.compressors = compressors.clone();
        // Make the database Read only as much as we have control to do
        source_client_options.read_concern = ReadConcern::majority().into();
        let source_client = Client::with_options(source_client_options)?;

        let mut target_client_options = ClientOptions::parse(&target_uri).await?;
        target_client_options.max_pool_size = max_pool_size.into();
        target_client_options.min_pool_size = min_pool_size.into();
        target_client_options.max_connecting = max_connecting.into();
        target_client_options.compressors = compressors;
        let target_client = Client::with_options(target_client_options)?;

        // target_client.warm_connection_pool().await;
        // source_client.warm_connection_pool().await;

        let source_db_name = self.get_db_name(&source_uri, self.source_db.clone())?;
        let target_db_name = self.get_db_name(&target_uri, self.target_db.clone())?;

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
        let collection_names: Vec<String> = self
            .processors
            .iter()
            .map(|p| p.collection_name().to_string())
            .collect();
        dbs.clear_target_collections(&collection_names)
            .await
            .expect("Expected to successfully drop target database collections before replication");

        let (task_sender, task_receiver) = mpsc::channel(self.config.thread_count);

        let manager = ReplicationManager {
            dbs,
            processors: self.processors,
            config: self.config,
            task_receiver,
            task_sender,
        };

        Ok(manager)
    }

    fn get_db_name(&self, uri: &str, db_name: Option<String>) -> TuxedoResult<String> {
        let parsed_db_name = self.parse_db_name_from_uri(uri)?;

        match parsed_db_name {
            Some(db_name) => Ok(db_name),
            None => match db_name {
                Some(db_name) => Ok(db_name),
                None => Err(TuxedoError::ConfigError(
                    "Could not parse database name from URI and no database name provided.".into(),
                )),
            },
        }
    }

    fn parse_db_name_from_uri(&self, url: &str) -> TuxedoResult<Option<String>> {
        let url = Url::parse(url).map_err(|e| TuxedoError::ConfigError(e.to_string()))?;
        let path = url.path();
        if path.is_empty() || path == "/" {
            return Ok(None);
        }

        // Remove leading '/' and split by '?' to get rid of any query parameters
        let db_name = path.trim_start_matches('/').split('?').next().unwrap();

        if db_name.is_empty() {
            Ok(None)
        } else {
            Ok(Some(db_name.to_string()))
        }
    }
}
