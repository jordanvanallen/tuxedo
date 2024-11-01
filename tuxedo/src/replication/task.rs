use super::types::{DatabasePair, ReplicationStrategy};
use crate::Mask;
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use mongodb::options::FindOptions;
use mongodb_model::MongoModel;
use rayon::prelude::*;
use std::marker::PhantomData;
use std::sync::Arc;

#[async_trait]
pub(crate) trait Task: Send {
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
pub(crate) struct ModelTask<T: MongoModel + Mask> {
    dbs: Arc<DatabasePair>,
    query_config: QueryConfig,
    strategy: ReplicationStrategy,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

pub(crate) struct ReplicatorTask<T: Send> {
    dbs: Arc<DatabasePair>,
    collection_name: String,
    query_config: QueryConfig,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

impl<T: Send> ReplicatorTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        collection_name: String,
        query_config: QueryConfig,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            collection_name,
            query_config,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

impl<T: MongoModel + Mask> ModelTask<T> {
    pub(crate) fn new(
        dbs: Arc<DatabasePair>,
        query_config: QueryConfig,
        strategy: ReplicationStrategy,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            query_config,
            strategy,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync> Task for ReplicatorTask<T> {
    async fn run(&self) {
        let records: Vec<Document> = match self.dbs.read_documents(&self.collection_name, &self.query_config).await {
            Ok(docs) => docs,
            Err(e) => { 
                println!("Failed to retrieve records from collection: `{}` using QueryConfig: {:?}. Encountered error: {}", &self.collection_name, &self.query_config, e);
                return
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. QueryConfig used: {:?}",
                &self.query_config
            );
            return;
        }

        if let Err(e) = self.dbs.write::<Document>(&self.collection_name, &records).await {
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
impl<T: MongoModel + Mask> Task for ModelTask<T> {
    async fn run(&self) {
        let mut records: Vec<T> = match self.dbs.read::<T>(&self.query_config).await {
            Ok(docs) => docs,
            Err(e) => { 
                println!("Failed to retrieve records from collection: `{}` using QueryConfig: {:?}. Encountered error: {e}", T::COLLECTION_NAME, &self.query_config);
                return
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

        if let Err(e) = self.dbs.write(T::COLLECTION_NAME, &records).await {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using QueryConfig: {:?}. Encountered error: {e}",
                records.len(), 
                T::COLLECTION_NAME, 
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
