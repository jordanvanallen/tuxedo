use super::types::{DatabasePair, ReplicationStrategy};
use crate::Mask;
use async_trait::async_trait;
use bson::Document;
use indicatif::ProgressBar;
use mongodb::options::{FindOptions, InsertManyOptions};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

// Define a write batch size constant
const WRITE_BATCH_SIZE: usize = 1000;

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
        // Get the cursor
        let mut cursor = match self
            .dbs
            .read_documents(
                &self.collection_name,
                self.config.query.clone(),
                self.config.read_options.clone().into(),
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                println!(
                    "Failed to retrieve cursor for collection: `{}` using Query: {:?} with read options: {:?}. Encountered error: {}",
                    &self.collection_name,
                    &self.config.query,
                    &self.config.read_options,
                    e
                );
                return;
            }
        };

        let mut write_batch: Vec<Document> = Vec::with_capacity(WRITE_BATCH_SIZE);
        let mut total_processed = 0;

        // Iterate using advance() and deserialize_current()
        while match cursor.advance().await {
            Ok(true) => true, // Advanced successfully, okay to deserialize
            Ok(false) => false, // End of cursor
            Err(e) => {
                println!(
                    "Error advancing cursor for collection: `{}`. Stopping task. Error: {}",
                    &self.collection_name, e
                );
                false // Stop processing loop
            }
        } {
            // If advance returned Ok(true), we can deserialize the current document
            // Deserialize the current document using the faster method
            let mut doc = match cursor.deserialize_current() {
                Ok(d) => d,
                Err(e) => {
                    println!(
                        "Failed to deserialize document for collection: `{}`. Skipping document. Error: {}",
                        &self.collection_name, e
                    );
                    continue; // Skip this document
                }
            };

            // Apply masking if lambda exists
            if let Some(masking_fn) = self.masking_lambda.as_ref() {
                (masking_fn)(&mut doc);
            }

            write_batch.push(doc);
            total_processed += 1;

            // Write in batches
            if write_batch.len() >= WRITE_BATCH_SIZE {
                if let Err(e) = self
                    .dbs
                    .write::<Document>(
                        &self.collection_name,
                        &write_batch,
                        self.config.write_options.clone().into(),
                    )
                    .await
                {
                    println!(
                        "Failed to insert batch of {} records into collection: `{}`. Error: {}",
                        write_batch.len(),
                        &self.collection_name,
                        e
                    );
                    // Decide how to handle batch write errors
                } else {
                    self.update_progress_bar(&self.progress_bar, write_batch.len());
                }
                write_batch.clear();
            }
        }

        // Write any remaining documents
        if !write_batch.is_empty() {
            if let Err(e) = self
                .dbs
                .write::<Document>(
                    &self.collection_name,
                    &write_batch,
                    self.config.write_options.clone().into(),
                )
                .await
            {
                println!(
                    "Failed to insert final batch of {} records into collection: `{}`. Error: {}",
                    write_batch.len(),
                    &self.collection_name,
                    e
                );
            } else {
                self.update_progress_bar(&self.progress_bar, write_batch.len());
            }
        }

        if total_processed == 0 {
            println!(
                "No records found or processed for batch. Query: {:?} with read options: {:?}",
                &self.config.query,
                &self.config.read_options,
            );
        }
    }
}

#[async_trait]
impl<T: Mask + Serialize + DeserializeOwned + Send + Sync + Unpin> Task for ModelTask<T> {
    async fn run(&self) {
        // Get the cursor
        let mut cursor = match self
            .dbs
            .read::<T>( // Use the typed read method
                        &self.collection_name,
                        self.config.query.clone(),
                        self.config.read_options.clone().into(),
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                println!(
                    "Failed to retrieve cursor for collection: `{}` using Query: {:?} with read options: {:?}. Encountered error: {}",
                    &self.collection_name,
                    &self.config.query,
                    &self.config.read_options,
                    e
                );
                return;
            }
        };

        let mut write_batch: Vec<T> = Vec::with_capacity(WRITE_BATCH_SIZE);
        let mut total_processed = 0;
        let use_masking = matches!(self.strategy, ReplicationStrategy::Mask);

        // Iterate using advance() and deserialize_current()
        while match cursor.advance().await {
            Ok(true) => true, // Advanced successfully, okay to deserialize
            Ok(false) => false, // End of cursor
            Err(e) => {
                println!(
                    "Error advancing cursor for collection: `{}`. Stopping task. Error: {}",
                    &self.collection_name, e
                );
                false // Stop processing loop
            }
        } {
            // If advance returned Ok(true), we can deserialize the current document
            // Deserialize the current document using the faster method
            let mut record = match cursor.deserialize_current() {
                Ok(d) => d,
                Err(e) => {
                    println!(
                        "Failed to deserialize document for collection: `{}`. Skipping document. Error: {}",
                        &self.collection_name, e
                    );
                    continue; // Skip this document
                }
            };

            // Apply masking if strategy requires it
            if use_masking {
                record.mask();
            }

            write_batch.push(record);
            total_processed += 1;

            // Write in batches
            if write_batch.len() >= WRITE_BATCH_SIZE {
                if let Err(e) = self
                    .dbs
                    .write::<T>( // Use typed write
                                 &self.collection_name,
                                 &write_batch,
                                 self.config.write_options.clone().into(),
                    )
                    .await
                {
                    println!(
                        "Failed to insert batch of {} records into collection: `{}`. Error: {}",
                        write_batch.len(),
                        &self.collection_name,
                        e
                    );
                    // TODO; Decide how to handle batch write errors
                    // For now just keep going
                } else {
                    self.update_progress_bar(&self.progress_bar, write_batch.len());
                }
                write_batch.clear();
            }
        }

        // Write any remaining documents
        if !write_batch.is_empty() {
            if let Err(e) = self
                .dbs
                .write::<T>( // Use typed write
                             &self.collection_name,
                             &write_batch,
                             self.config.write_options.clone().into(),
                )
                .await
            {
                println!(
                    "Failed to insert final batch of {} records into collection: `{}`. Error: {}",
                    write_batch.len(),
                    &self.collection_name,
                    e
                );
            } else {
                self.update_progress_bar(&self.progress_bar, write_batch.len());
            }
        }

        if total_processed == 0 {
            println!(
                "No records found or processed for batch. Query: {:?} with read options: {:?}",
                &self.config.query,
                &self.config.read_options,
            );
        }
    }
}
