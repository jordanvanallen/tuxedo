use super::types::ReplicationStrategy;
use crate::database::pagination::PaginationOptions;
use crate::database::{
    traits::{Destination, Source},
    DatabasePair,
};
use crate::Mask;
use async_trait::async_trait;
use indicatif::ProgressBar;
use mongodb::{bson::Document, options::FindOptions};
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

pub(crate) struct ModelTask<T, S, D>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Source,
    D: Destination,
{
    dbs: Arc<DatabasePair<S, D>>,
    entity_name: String,
    query: S::Query,
    pagination_options: PaginationOptions,
    strategy: ReplicationStrategy,
    progress_bar: Arc<ProgressBar>,
    _phantom_data: PhantomData<T>,
}

impl<T, S, D> ModelTask<T, S, D>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Source,
    D: Destination,
{
    pub(crate) fn new(
        dbs: Arc<DatabasePair<S, D>>,
        entity_name: impl Into<String>,
        query: S::Query,
        pagination_options: PaginationOptions,
        strategy: ReplicationStrategy,
        progress_bar: Arc<ProgressBar>,
    ) -> Self {
        Self {
            dbs,
            entity_name: entity_name.into(),
            query,
            pagination_options,
            strategy,
            progress_bar,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<T, S, D> Task for ModelTask<T, S, D>
where
    T: Mask + Serialize + DeserializeOwned + Send + Sync,
    S: Source,
    D: Destination,
{
    async fn run(&self) {
        let mut records: Vec<T> = match self
            .dbs
            .source
            .read_chunk::<T>(
                &self.entity_name,
                self.query.clone(),
                self.pagination_options.clone(),
            )
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                println!(
                    "Failed to retrieve records from collection: `{}` using Query: {} and PaginationOptions: {:?}. Encountered error: {e}",
                    &self.entity_name,
                    &self.query,
                    &self.pagination_options
                );
                return;
            }
        };

        if records.is_empty() {
            println!(
                "No records found for batch. Skipping insertion. Using query: {} and PaginationOptions: {:?}",
                &self.query,
                &self.pagination_options,
            );
            return;
        }

        match self.strategy {
            ReplicationStrategy::Clone => (),
            ReplicationStrategy::Mask => records.par_iter_mut().for_each(Mask::mask),
        }

        if let Err(e) = self
            .dbs
            .destination
            .write::<T>(&self.entity_name, &records).await {
            println!(
                "Failed to insert {} records into collection: `{}`. Records were retrieved using query: {} and PaginationOptions: {:?}. Encountered error: {e}",
                records.len(),
                &self.entity_name,
                &self.query,
                &self.pagination_options,
            );
            return;
        }

        self.update_progress_bar(&self.progress_bar, records.len());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryConfig<Q> {
    pub(crate) query: Q,
    pub(crate) skip: usize,
    pub(crate) limit: usize,
    pub(crate) batch_size: usize,
}

impl<Q> QueryConfig<Q> {
    pub(crate) fn new(query: Q, skip: usize, limit: usize, batch_size: usize) -> Self {
        Self {
            query,
            skip,
            limit,
            batch_size,
        }
    }
}

impl QueryConfig<Document> {
    pub(crate) fn mongo_find_options(&self) -> FindOptions {
        FindOptions::builder()
            .limit(self.limit as i64)
            .skip(self.skip as u64)
            .batch_size(self.batch_size as u32)
            .build()
    }
}
