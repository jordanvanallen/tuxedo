use crate::database::mongodb::{get_compressors, MongodbSource};
use crate::{TuxedoError, TuxedoResult};
use mongodb::options::{ClientOptions, Compressor, CountOptions, FindOptions};
use mongodb::Client;

const DEFAULT_BATCH_SIZE: u64 = 5000;

#[derive(Default, Clone)]
pub struct MongodbSourceBuilder {
    database_name: Option<String>,
    client_options: Option<ClientOptions>,
    read_options: Option<FindOptions>,
    count_options: Option<CountOptions>,
    batch_size: Option<u64>,
    cursor_batch_size: Option<u32>,
    compressors: Option<Vec<Compressor>>,
}

impl MongodbSourceBuilder {
    pub fn new() -> MongodbSourceBuilder {
        MongodbSourceBuilder::default()
    }

    pub fn database_name(mut self, database_name: impl Into<String>) -> Self {
        self.database_name = Some(database_name.into());
        self
    }

    pub fn client_options(mut self, options: impl Into<Option<ClientOptions>>) -> Self {
        self.client_options = options.into();
        self
    }

    pub fn read_options(mut self, options: impl Into<Option<FindOptions>>) -> Self {
        self.read_options = options.into();
        self
    }

    pub fn count_options(mut self, options: impl Into<Option<CountOptions>>) -> Self {
        self.count_options = options.into();
        self
    }

    pub fn batch_size(mut self, batch_size: impl Into<u64>) -> Self {
        self.batch_size = Some(batch_size.into());
        self
    }

    /// Set the MongoDB cursor batch size for improved performance
    ///
    /// The batch size controls how many documents are fetched from MongoDB in each round trip.
    /// Larger batch sizes can significantly improve performance by reducing network overhead,
    /// particularly for documents of small to medium size.
    ///
    /// Recommended values:
    /// - 1000-2000 for small documents (< 1KB)
    /// - 500-1000 for medium documents (1-10KB)
    /// - 100-500 for large documents (> 10KB)
    ///
    /// If not specified, an optimized default based on the thread count will be used.
    pub fn cursor_batch_size(mut self, batch_size: u32) -> Self {
        self.cursor_batch_size = Some(batch_size);
        self
    }

    /// Set cursor batch size to match the processing batch size
    ///
    /// This aligns the MongoDB cursor batch size with the batch size used for processing documents.
    /// Aligning these values can improve performance by ensuring that network fetching matches
    /// the processing pattern.
    ///
    /// For most workloads, using a batch size 20% larger than the processing batch size works well.
    pub fn align_with_batch_size(mut self) -> Self {
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        // Simple cast - batch sizes are always reasonably sized numbers
        self.cursor_batch_size = Some((batch_size * 1.2 as u64) as u32);
        self
    }

    /// Enable network compression for improved performance
    ///
    /// Enabling compression can significantly reduce network bandwidth usage and
    /// improve performance, especially for:
    /// - Large documents
    /// - Remote or high-latency connections
    /// - Network-constrained environments
    ///
    /// Note: For local connections or same-datacenter scenarios, compression may
    /// actually decrease performance due to CPU overhead of compression/decompression.
    ///
    /// Available compression algorithms (in recommended order):
    /// - Zstd (best compression ratio)
    /// - Zlib (good balance of speed and compression)
    /// - Snappy (fastest, but less compression)
    ///
    /// This method enables all available compressors, letting MongoDB negotiate
    /// the best supported algorithm with the server.
    pub fn enable_compression(mut self) -> Self {
        // Enable all available compressors in order of preference
        // MongoDB will negotiate the best supported algorithm with the server
        self.compressors = get_compressors();
        self
    }

    /// Configure for maximum performance across all settings
    ///
    /// This convenience method applies performance optimizations:
    /// - Uses the cursor_batch_size if already set, otherwise uses default of DEFAULT_BATCH_SIZE
    /// - Optionally enables network compression (for cross-datacenter scenarios)
    /// - Optimizes connection pool settings
    ///
    /// Note: This method should be called right before build() to ensure all settings are applied correctly.
    //        However, you can also call it earlier then override optimization values for your environment
    ///
    /// @param enable_compression Set to true for cross-datacenter scenarios where
    ///                           compression benefits outweigh the CPU overhead
    pub fn optimize_for_performance(&self, enable_compression: bool) -> Self {
        // Use a mut builder variable to avoid moving self
        // The overhead here is so low it's not really going to matter
        // but makes things a bit simpler
        let mut builder = self.clone();

        // Enable compression only if specified (recommended for cross-datacenter scenarios)
        if enable_compression {
            builder = builder.enable_compression();
        }

        // If cursor batch size wasn't already set, use a reasonable default
        if self.cursor_batch_size.is_none() {
            builder = builder.align_with_batch_size();
        }

        builder
    }

    pub async fn build(self) -> TuxedoResult<MongodbSource> {
        let database_name = self.database_name.ok_or_else(|| {
            TuxedoError::Generic("No database name provided for mongodb source database".into())
        })?;

        let mut client_options = if let Some(options) = self.client_options {
            options
        } else {
            return Err(TuxedoError::Generic(
                "No client options provided for mongodb source database".into(),
            ));
        };

        // Apply smart defaults for connection pool settings if not already specified
        if client_options.max_pool_size.is_none() {
            // Default to thread count + buffer
            let thread_count = num_cpus::get() as u32;
            client_options.max_pool_size = Some(std::cmp::max(thread_count * 2, 8));
        }
        if client_options.min_pool_size.is_none() {
            client_options.min_pool_size = Some(num_cpus::get() as u32);
        }

        // Apply compression settings if specified
        if let Some(compressors) = self.compressors {
            client_options.compressors = Some(compressors);
        }

        let client = Client::with_options(client_options)?;
        let db = client.database(database_name.as_str());

        // Apply read options with optimized batch size
        let mut read_options: FindOptions = self.read_options.unwrap_or_default();

        // Only set batch size if user hasn't explicitly configured it
        if read_options.batch_size.is_none() {
            // Use user-provided batch size or use default
            let batch_size = self.cursor_batch_size.unwrap_or(1000);
            read_options.batch_size = Some(batch_size);
        }

        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        MongodbSource::new(client, db, read_options, self.count_options, batch_size).await
    }
}
