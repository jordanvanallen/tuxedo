# Tuxedo &emsp;

**Tuxedo is a library for masking and transferring your MongoDB database collections between instances**

**This library is currently in production and can regularly experience breaking changes**

## Installation

```toml
[dependencies]
tuxedo = { version = "0.5.0" }
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
        .copy_views(true)
        .add_processor::<User>("users")
        .build()
        .await?;

    replication_manager.run().await?;

    Ok(())
}
```

### Processors

Processors are used for collections that need to be masked.

They can be configured individually using a `ProcessorConfigBuilder` alongside the `add_processor_with_config` function. This is required for things like discriminators/polymorphic models, and can be done by overriding the `query` as needed to ensure no duplicate entries are created.

### Replicators

Replicators are used for collections that need to be replicated, but do not need to be masked. They have the benefit of not requiring a struct to replicate the data, but are also significantly slower as they as (de)serialized using a bson::Document, which is much less ideal then a defined struct. It is recommended for larger collections to use a struct and define the `Mask` trait with a NOP to avoid the masking portion, but allow for much faster replication speeds.

### Views

MongoDB views can be copied from source to target databases using the `copy_views(true)` configuration option. Views are automatically detected from the source database and recreated in the target database after all collections and indexes have been processed. This includes the view's underlying collection reference and aggregation pipeline.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
