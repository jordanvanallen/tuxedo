use super::{get_compressors, MongodbDestination};
use crate::{TuxedoError, TuxedoResult};
use mongodb::options::{ClientOptions, Compressor, InsertManyOptions};
use mongodb::Client;

#[derive(Default, Clone)]
pub struct MongodbDestinationBuilder {
    database_name: Option<String>,
    client_options: Option<ClientOptions>,
    write_options: Option<InsertManyOptions>,
    compressors: Option<Vec<Compressor>>,
}

impl MongodbDestinationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn database_name(mut self, database_name: &str) -> Self {
        self.database_name = Some(database_name.to_string());
        self
    }

    pub fn client_options(mut self, client_options: impl Into<ClientOptions>) -> Self {
        self.client_options = Some(client_options.into());
        self
    }

    pub fn write_options(mut self, write_options: impl Into<InsertManyOptions>) -> Self {
        self.write_options = Some(write_options.into());
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

    /// Configure for high throughput writes
    ///
    /// This sets write options for maximum write performance:
    /// - Unordered writes (continue on error)
    /// - Optimized write concern
    ///
    /// Note: This prioritizes performance over guaranteed durability.
    pub fn high_throughput_writes(mut self) -> Self {
        let mut options = self.write_options.unwrap_or_default();

        // Use unordered writes (continue processing on error)
        options.ordered = Some(false);

        // Disable document validation for better performance
        //
        // This is fine because we are assuming the data is already validated
        // in the source database so we don't need to validate it again
        options.bypass_document_validation = Some(true);

        self.write_options = Some(options);
        self
    }

    /// Configure for maximum performance across all settings
    ///
    /// This convenience method applies performance optimizations:
    /// - Optimized write options
    /// - Optionally enables network compression (for cross-datacenter scenarios)
    ///
    /// @param enable_compression Set to true for cross-datacenter scenarios where
    ///                           compression benefits outweigh the CPU overhead
    pub fn optimize_for_performance(&self, enable_compression: bool) -> Self {
        let mut builder = self.clone();

        // Apply high throughput write settings directly
        builder = builder.high_throughput_writes();

        // Apply compression settings directly
        if enable_compression {
            builder = builder.enable_compression();
        }

        builder
    }

    pub async fn build(self) -> TuxedoResult<MongodbDestination> {
        let database_name = self.database_name.ok_or_else(|| {
            TuxedoError::Generic(
                "No database name provided for mongodb destination database".into(),
            )
        })?;

        let mut client_options = if let Some(options) = self.client_options {
            options
        } else {
            return Err(TuxedoError::Generic(
                "No client options provided for mongodb destination database".into(),
            ));
        };

        // Apply smart defaults for connection pool settings if not already specified
        if client_options.max_pool_size.is_none() {
            // Default to thread count + buffer
            let thread_count = num_cpus::get() as u32;
            client_options.max_pool_size = Some(std::cmp::max(thread_count + 5, 10));
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

        Ok(MongodbDestination::new(client, db, self.write_options))
    }
}
