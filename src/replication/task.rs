use super::types::{DatabasePair, ReplicationStrategy};
use crate::Mask;
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use mongodb::options::{FindOptions, InsertManyOptions};
use rayon::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

#[async_trait]
pub(crate) trait Task: Send + Sync {
    async fn run(&self);
    fn update_progress_bar(&self, progress_bar: &ProgressBar, num_records: usize) {
        progress_bar.inc(num_records as u64);
        if progress_bar.is_finished() {
            progress_bar.finish_and_clear();
            progress_bar.set_message("Complete");
        }
    }
}

#[derive(Debug)]
pub(crate) struct ModelTask<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static> {
    dbs: Arc<DatabasePair>,
    collection_name: String,
    config: TaskConfig,
    progress_bar: Arc<ProgressBar>,
    strategy: ReplicationStrategy,
    _phantom_data: PhantomData<T>,
}

pub(crate) struct ReplicatorTask<T: Send> {
    dbs: Arc<DatabasePair>,
    collection_name: String,
    config: TaskConfig,
    masking_lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

#[derive(Debug)]
pub(crate) struct TaskConfig {
    pub(crate) query: Document,
    pub(crate) read_options: FindOptions,
    pub(crate) write_options: InsertManyOptions,
}

impl<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static> ModelTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        collection_name: impl Into<String>,
        config: TaskConfig,
        strategy: ReplicationStrategy,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            collection_name: collection_name.into(),
            config,
            strategy,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

impl<T: Send> ReplicatorTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        collection_name: impl Into<String>,
        config: TaskConfig,
        masking_lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            collection_name: collection_name.into(),
            config,
            masking_lambda,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync> Task for ReplicatorTask<T> {
    async fn run(&self) {
        let mut records: Vec<Document> = match self
            .dbs
            .read(
                &self.collection_name,
                self.config.query.clone(),
                self.config.read_options.clone().into(),
            )
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                println!(
                    "Failed to retrieve records from collection: `{}` using Query: {:?} with read options: {:?}. Encountered error: {}",
                    &self.collection_name,
                    &self.config.query,
                    &self.config.read_options,
                    e
                );
                return;
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. Query: {:?} with read options: {:?}",
                &self.config.query,
                &self.config.read_options,
            );
            return;
        }

        if let Some(masking_fn) = self.masking_lambda.clone() {
            records.par_iter_mut().for_each(|record| {
                (masking_fn)(record);
            });
        }

        if let Err(e) = self
            .dbs
            .write::<Document>(
                &self.collection_name,
                &records,
                self.config.write_options.clone().into(),
            )
            .await
        {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using Query: {:?} with read options: {:?}. Write options were: {:?}. Encountered error: {e}",
                records.len(),
                &self.collection_name,
                &self.config.query,
                &self.config.read_options,
                &self.config.write_options,
            );
            return;
        }

        self.update_progress_bar(&self.progress_bar, records.len());
    }
}

#[async_trait]
impl<T: Mask + Serialize + DeserializeOwned + Send + Sync> Task for ModelTask<T> {
    async fn run(&self) {
        let mut records: Vec<T> = match self
            .dbs
            .read(
                &self.collection_name,
                self.config.query.clone(),
                self.config.read_options.clone().into(),
            )
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                println!(
                    "Failed to retrieve records from collection: `{}` using Query: {:?} with read options: {:?}. Encountered error: {}",
                    &self.collection_name,
                    &self.config.query,
                    &self.config.read_options,
                    e
                );
                return;
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. Query: {:?} with read options: {:?}",
                &self.config.query,
                &self.config.read_options,
            );
            return;
        }

        match self.strategy {
            ReplicationStrategy::Clone => (),
            ReplicationStrategy::Mask => records.par_iter_mut().for_each(Mask::mask),
        }

        if let Err(e) = self
            .dbs
            .write::<T>(
                &self.collection_name,
                &records,
                self.config.write_options.clone().into(),
            )
            .await
        {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using Query: {:?} with read options: {:?}. Write options were: {:?}. Encountered error: {e}",
                records.len(),
                &self.collection_name,
                &self.config.query,
                &self.config.read_options,
                &self.config.write_options,
            );
            return;
        }

        self.update_progress_bar(&self.progress_bar, records.len());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryConfig {
    pub(crate) query: Document,
    pub(crate) read_options: Option<FindOptions>,
}

impl QueryConfig {
    pub(crate) fn new(query: Document, read_options: Option<FindOptions>) -> Self {
        Self {
            query,
            read_options,
        }
    }
}
