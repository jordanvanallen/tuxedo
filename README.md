# Tuxedo &emsp;

**Tuxedo is a library for masking and transferring your MongoDB database collections between instances**

**This library is currently in production and can regularly experience breaking changes**

## Installation

```toml
[dependencies]
tuxedo = { version = "0.3.1" }
```

## Usage

```rust
use tuxedo::{
  Mask, ProcessorConfigBuilder, ReplicationManagerBuilder, ReplicationStrategy, TuxedoResult,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub first_name: String,
    pub last_name: String,
}

impl Mask for User {
    fn mask(&mut self) {
        self.first_name = Self::fake_first_name();
        self.last_name = Self::fake_last_name();
    }
}

#[tokio::main]
async fn main() -> TuxedoResult<()> {
    let replication_manager = ReplicationManagerBuilder::new()
        .source_uri("mongodb://localhost:27017")
        .target_uri("mongodb://localhost:27016")
        .source_db("source_db_name")
        .target_db("target_db_name")
        .batch_size(2500 as usize)
        .strategy(ReplicationStrategy::Mask) 
        .add_processor::<User>("users")
        .build()
        .await?;

    replication_manager.run().await?;

    Ok(())
}
```

### Performance Optimizations

For better performance with varying document sizes, you can use adaptive batch sizing:

```rust
let replication_manager = ReplicationManagerBuilder::new()
    .source_uri("mongodb://localhost:27017")
    .target_uri("mongodb://localhost:27016")
    .source_db("source_db_name")
    .target_db("target_db_name")
    .strategy(ReplicationStrategy::Mask)
    // Enable adaptive batch sizing for all collections
    .with_adaptive_batch_sizing()
    // Optionally override the automatically calculated target batch size
    .with_target_batch_bytes(4 * 1024 * 1024) // 4MB batches
    .add_processor::<User>("users")
    .add_processor::<LargeDocument>("large_documents")
    .build()
    .await?;
```

When adaptive batch sizing is enabled without an explicit target size, Tuxedo will:

1. Sample documents to determine average document size for each collection
2. Automatically choose optimal batch size targets based on document size:
   - Tiny documents (<1KB): 12MB batch size
   - Small documents (1KB-10KB): 8MB batch size
   - Medium documents (10KB-100KB): 4MB batch size
   - Large documents (100KB-500KB): 2MB batch size
   - Very large documents (>500KB): 1MB batch size
3. Calculate the optimal number of documents per batch
4. Apply reasonable limits (between 100-10000 documents)

This ensures that collections with very different document sizes are processed efficiently. For example, a collection with tiny documents might process 10,000 documents per batch, while a collection with large 600KB documents might only process 100 documents per batch.

You can also set adaptive batch sizing on individual processors:

```rust
// For individual processor configuration
let user_config = ProcessorConfigBuilder::default()
    .adaptive_batch_size(true)
    .target_batch_bytes(Some(2 * 1024 * 1024)) // 2MB batch size for users
    .build();

let large_doc_config = ProcessorConfigBuilder::default()
    .adaptive_batch_size(true)
    .target_batch_bytes(Some(10 * 1024 * 1024)) // 10MB batch size for large docs
    .build();

let replication_manager = ReplicationManagerBuilder::new()
    // ... other settings ...
    .add_processor_with_config::<User>("users", user_config)
    .add_processor_with_config::<LargeDocument>("large_documents", large_doc_config)
    .build()
    .await?;
```

### Processors

Processors are used for collections that need to be masked.

They can be configured individually using a `ProcessorConfigBuilder` alongside the `add_processor_with_config` function. This is required for things like discriminators/polymorphic models, and can be done by overriding the `query` as needed to ensure no duplicate entries are created.

### Replicators

Replicators are used for collections that need to be replicated, but do not need to be masked. They have the benefit of not requiring a struct to replicate the data, but are also significantly slower as they as (de)serialized using a bson::Document, which is much less ideal then a defined struct. It is recommended for larger collections to use a struct and define the `Mask` trait with a NOP to avoid the masking portion, but allow for much faster replication speeds.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
