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

// TODO: impl Into<FindOptions> for the config? Can we somehow make it into a tuple the
// function will accept with a splat? is that gross?
pub(crate) struct ModelTask<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static> {
    dbs: Arc<DatabasePair>,
    collection_name: String,
    query_config: QueryConfig,
    write_config: WriteConfig,
    strategy: ReplicationStrategy,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

pub(crate) struct ReplicatorTask<T: Send> {
    dbs: Arc<DatabasePair>,
    collection_name: String,
    query_config: QueryConfig,
    write_config: WriteConfig,
    masking_lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

impl<T: Send> ReplicatorTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        collection_name: impl Into<String>,
        query_config: QueryConfig,
        write_config: WriteConfig,
        masking_lambda: Option<Arc<dyn Fn(&mut Document) + Send + Sync>>,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            collection_name: collection_name.into(),
            query_config,
            write_config,
            masking_lambda,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

impl<T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static> ModelTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        collection_name: impl Into<String>,
        query_config: QueryConfig,
        write_config: WriteConfig,
        strategy: ReplicationStrategy,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            collection_name: collection_name.into(),
            query_config,
            write_config,
            strategy,
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
            .read_documents(&self.collection_name, &self.query_config)
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                println!("Failed to retrieve records from collection: `{}` using QueryConfig: {:?}. Encountered error: {}", &self.collection_name, &self.query_config, e);
                return;
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. QueryConfig used: {:?}",
                &self.query_config
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
            .write::<Document>(&self.collection_name, &self.write_config, &records)
            .await
        {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using QueryConfig: {:?}. Encountered error: {e}",
                records.len(),
                &self.collection_name,
                &self.query_config,
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
            .read::<T>(&self.collection_name, &self.query_config)
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                println!("Failed to retrieve records from collection: `{}` using QueryConfig: {:?}. Encountered error: {e}", &self.collection_name, &self.query_config);
                return;
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. QueryConfig used: {:?}",
                &self.query_config
            );
            return;
        }

        match self.strategy {
            ReplicationStrategy::Clone => (),
            ReplicationStrategy::Mask => records.par_iter_mut().for_each(Mask::mask),
        }

        if let Err(e) = self
            .dbs
            .write(&self.collection_name, &self.write_config, &records)
            .await
        {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using QueryConfig: {:?}. Encountered error: {e}",
                records.len(),
                &self.collection_name,
                &self.query_config,
            );
            return;
        }

        self.update_progress_bar(&self.progress_bar, records.len());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryConfig {
    pub(crate) query: Document,
    pub(crate) skip: usize,
    pub(crate) limit: usize,
    pub(crate) batch_size: usize,
}

impl QueryConfig {
    pub(crate) fn new(query: Document, skip: usize, limit: usize, batch_size: usize) -> Self {
        Self {
            query,
            skip,
            limit,
            batch_size,
        }
    }

    pub(crate) fn mongo_find_options(&self) -> FindOptions {
        FindOptions::builder()
            .limit(self.limit as i64)
            .skip(self.skip as u64)
            .batch_size(self.batch_size as u32)
            .build()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WriteConfig {
    pub(crate) bypass_document_validation: bool,
}

impl WriteConfig {
    pub(crate) fn new(bypass_document_validation: bool) -> Self {
        Self {
            bypass_document_validation,
        }
    }

    pub(crate) fn insert_many_options(&self) -> InsertManyOptions {
        InsertManyOptions::builder()
            .bypass_document_validation(self.bypass_document_validation)
            .build()
    }
}
