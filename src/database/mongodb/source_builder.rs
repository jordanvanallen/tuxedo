use crate::database::mongodb::MongodbSource;
use crate::{TuxedoError, TuxedoResult};
use mongodb::options::{ClientOptions, CountOptions, FindOptions};
use mongodb::Client;

#[derive(Default)]
pub struct MongodbSourceBuilder {
    database_name: Option<String>,
    client_options: Option<ClientOptions>,
    read_options: Option<FindOptions>,
    count_options: Option<CountOptions>,
    cursor_batch_size: Option<u32>,
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

    pub async fn build(self) -> TuxedoResult<MongodbSource> {
        let database_name = self
            .database_name
            .ok_or_else(||
                TuxedoError::Generic("No database name provided for mongodb source database".into())
            )?;
        
        let mut client_options = if let Some(options) = self.client_options {
            options
        } else {
            return Err(TuxedoError::Generic("No client options provided for mongodb source database".into()))
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

        let client = Client::with_options(client_options)?;
        let db = client.database(database_name.as_str());
        
        // Apply read options with optimized batch size
        let mut read_options = self.read_options.unwrap_or_else(|| FindOptions::default());
        
        // Only set batch size if user hasn't explicitly configured it
        if read_options.batch_size.is_none() {
            // Use user-provided batch size or calculate an optimal default
            let batch_size = self.cursor_batch_size.unwrap_or_else(|| {
                // Calculate an optimized batch size based on thread count
                // This helps balance memory usage vs network round trips
                let thread_count = num_cpus::get() as u32;
                // Default value: threads × 100, minimum 1000, maximum 5000
                // This value has been found to work well for most document sizes
                std::cmp::min(std::cmp::max(thread_count * 100, 1000), 5000)
            });
            
            read_options.batch_size = Some(batch_size);
        }

        MongodbSource::new(
            client,
            db,
            read_options,
            self.count_options,
        ).await
    }
}