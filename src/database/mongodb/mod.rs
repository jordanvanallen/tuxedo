pub use destination::MongodbDestination;
pub use destination_builder::MongodbDestinationBuilder;
use mongodb::options::Compressor;
pub use source::MongodbSource;
pub use source_builder::MongodbSourceBuilder;

pub mod destination;
pub mod destination_builder;
pub mod source;
pub mod source_builder;
pub mod conversions;

/// Available compression algorithms (in recommended order):
/// - Zstd (best compression ratio)
/// - Zlib (good balance of speed and compression)
/// - Snappy (fastest, but less compression)
pub(crate) fn get_compressors() -> Option<Vec<Compressor>> {
    Some(vec![
        // Zstd offers the best compression ratio and good performance
        Compressor::Zstd { level: None },
        // Zlib is widely supported with good compression
        Compressor::Zlib { level: None },
        // Snappy is fastest but has lower compression ratio
        Compressor::Snappy,
    ])
}

#[cfg(test)]
mod tests {
    //! Tests for MongoDB index conversions
    //!
    //! These tests verify that our conversion layer correctly translates 
    //! between our internal index representations and MongoDB's native formats.
    //! The tests cover all supported index types:
    //! - Standard indexes
    //! - Unique indexes
    //! - Text indexes with language options
    //! - Geospatial indexes (2dsphere and legacy 2d)
    //! - Hashed indexes
    //!
    //! Both directions are tested to ensure bi-directional compatibility.

    use crate::database::index::{IndexConfig, IndexDirection, IndexField, IndexType, SourceIndexes};
    use bson;
    use mongodb::{options::IndexOptions, IndexModel};
    use serde_json;
    use std::collections::HashMap;

    // Helper functions to make tests more maintainable

    /// Creates a standard (non-unique) index configuration for testing
    fn create_standard_index_config(name: &str, field_name: &str) -> IndexConfig {
        IndexConfig {
            name: name.to_string(),
            fields: vec![
                IndexField {
                    name: field_name.to_string(),
                    direction: IndexDirection::Ascending,
                }
            ],
            index_type: IndexType::Standard,
            options: HashMap::new(),
        }
    }

    /// Creates a unique index configuration for testing
    fn create_unique_index_config(name: &str, field_name: &str) -> IndexConfig {
        IndexConfig {
            name: name.to_string(),
            fields: vec![
                IndexField {
                    name: field_name.to_string(),
                    direction: IndexDirection::Ascending,
                }
            ],
            index_type: IndexType::Unique,
            options: HashMap::new(),
        }
    }

    /// Verifies common properties of an index model, reducing duplication
    fn assert_index_model_basics(index_model: &IndexModel, expected_name: &str, expected_field: &str) {
        let options = index_model.options.as_ref().expect("Index should have options");
        assert_eq!(options.name, Some(expected_name.to_string()), "Index name should match");
        assert!(index_model.keys.contains_key(expected_field), "Index should contain expected field");
    }

    /// Tests conversion from a standard index config to MongoDB IndexModel
    #[test]
    fn test_index_conversion() {
        // Create a test index config
        let config = create_standard_index_config("idx_test_name_asc", "name");

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_name_asc", "name");

        // Check the direction value
        let field_value = index_model.keys.get("name").expect("Field should exist");
        assert_eq!(field_value.as_i32().expect("Should be an integer"), 1, "Direction should be ascending (1)");
    }

    /// Tests conversion from a unique index config to MongoDB IndexModel
    #[test]
    fn test_unique_index_conversion() {
        // Create a test unique index config
        let config = create_unique_index_config("idx_test_email_asc", "email");

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_email_asc", "email");

        // Check unique option
        let options = index_model.options.as_ref().expect("Index should have options");
        assert_eq!(options.unique, Some(true), "Index should be unique");

        // Check the direction value
        let field_value = index_model.keys.get("email").expect("Field should exist");
        assert_eq!(field_value.as_i32().expect("Should be an integer"), 1, "Direction should be ascending (1)");
    }

    /// Tests conversion from SourceIndexes (multiple indexes) to Vec<IndexModel>
    #[test]
    fn test_source_indexes_conversion() {
        // Create test source indexes
        let source_indexes = SourceIndexes {
            entity_name: "users".to_string(),
            indexes: vec![
                create_standard_index_config("idx_users_name_asc", "name"),
                create_unique_index_config("idx_users_email_asc", "email"),
            ],
        };

        // Convert to Vec<IndexModel> using the From implementation
        let index_models: Vec<IndexModel> = Vec::from(source_indexes);

        // Verify the conversion
        assert_eq!(index_models.len(), 2, "Should have 2 index models");

        // First index (standard)
        assert_index_model_basics(&index_models[0], "idx_users_name_asc", "name");

        // Second index (unique)
        assert_index_model_basics(&index_models[1], "idx_users_email_asc", "email");
        let options = index_models[1].options.as_ref().expect("Index should have options");
        assert_eq!(options.unique, Some(true), "Second index should be unique");
    }

    /// Tests conversion from a 2dsphere geospatial index config to MongoDB IndexModel
    #[test]
    fn test_geo2dsphere_index_conversion() {
        // Create a test geo index config
        let config = IndexConfig {
            name: "idx_test_location_geo".to_string(),
            fields: vec![
                IndexField {
                    name: "location".to_string(),
                    direction: IndexDirection::Ascending, // Direction doesn't matter for geo indexes
                }
            ],
            index_type: IndexType::Geo2DSphere,
            options: HashMap::new(),
        };

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_location_geo", "location");

        // Check the field type for 2dsphere
        let field_value = index_model.keys.get("location").expect("Field should exist");
        assert_eq!(field_value.as_str().expect("Should be a string"), "2dsphere", "Field should be 2dsphere type");
    }

    /// Tests conversion from a legacy 2d geospatial index config to MongoDB IndexModel
    #[test]
    fn test_geo2d_index_conversion() {
        // Create a test 2d geo index config
        let config = IndexConfig {
            name: "idx_test_legacy_location_geo2d".to_string(),
            fields: vec![
                IndexField {
                    name: "legacyLocation".to_string(),
                    direction: IndexDirection::Ascending, // Direction doesn't matter for geo indexes
                }
            ],
            index_type: IndexType::Geo2D,
            options: HashMap::new(),
        };

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_legacy_location_geo2d", "legacyLocation");

        // Check the field type for 2d
        let field_value = index_model.keys.get("legacyLocation").expect("Field should exist");
        assert_eq!(field_value.as_str().expect("Should be a string"), "2d", "Field should be 2d type");
    }

    /// Tests conversion from a hashed index config to MongoDB IndexModel
    #[test]
    fn test_hashed_index_conversion() {
        // Create a test hashed index config
        let config = IndexConfig {
            name: "idx_test_userid_hashed".to_string(),
            fields: vec![
                IndexField {
                    name: "userId".to_string(),
                    direction: IndexDirection::Ascending, // Direction doesn't matter for hashed indexes
                }
            ],
            index_type: IndexType::Hashed,
            options: HashMap::new(),
        };

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_userid_hashed", "userId");

        // Check the field type for hashed
        let field_value = index_model.keys.get("userId").expect("Field should exist");
        assert_eq!(field_value.as_str().expect("Should be a string"), "hashed", "Field should be hashed type");
    }

    /// Tests conversion from a text index config to MongoDB IndexModel
    #[test]
    fn test_text_index_conversion() {
        // Create a test text index config with language options
        let mut options = HashMap::new();
        options.insert("default_language".to_string(), serde_json::Value::String("english".to_string()));
        options.insert("language_override".to_string(), serde_json::Value::String("lang".to_string()));

        let config = IndexConfig {
            name: "idx_test_description_text".to_string(),
            fields: vec![
                IndexField {
                    name: "description".to_string(),
                    direction: IndexDirection::Ascending, // Direction doesn't matter for text indexes
                }
            ],
            index_type: IndexType::Text,
            options,
        };

        // Convert to IndexModel
        let index_model = IndexModel::from(&config);

        // Verify the conversion
        assert_index_model_basics(&index_model, "idx_test_description_text", "description");

        // Check the field type for text
        let field_value = index_model.keys.get("description").expect("Field should exist");
        assert_eq!(field_value.as_str().expect("Should be a string"), "text", "Field should be text type");

        // Check language options
        let options = index_model.options.as_ref().expect("Index should have options");
        assert_eq!(options.default_language, Some("english".to_string()), "Should have English as default language");
        assert_eq!(options.language_override, Some("lang".to_string()), "Should have language override field");
    }

    /// Tests conversion from MongoDB IndexModel to our IndexConfig
    #[test]
    fn test_index_model_to_index_config_conversion() {
        // Create a MongoDB IndexModel directly
        let mut keys = bson::Document::new();
        keys.insert("email".to_string(), bson::Bson::Int32(1));

        let mut options = IndexOptions::default();
        options.name = Some("idx_users_email_asc".to_string());
        options.unique = Some(true);

        let mut index_model = IndexModel::builder().keys(keys).build();
        index_model.options = Some(options);

        // Convert to our IndexConfig
        let config = IndexConfig::from(index_model);

        // Verify the conversion
        assert_eq!(config.name, "idx_users_email_asc", "Name should match");
        assert_eq!(config.index_type, IndexType::Unique, "Type should be unique");
        assert_eq!(config.fields.len(), 1, "Should have 1 field");
        assert_eq!(config.fields[0].name, "email", "Field name should match");
        assert!(matches!(config.fields[0].direction, IndexDirection::Ascending), "Direction should be ascending");
        assert!(config.options.contains_key("unique"), "Should have unique option");
        assert_eq!(config.options["unique"], serde_json::Value::Bool(true), "Unique should be true");
    }

    /// Tests conversion from special MongoDB index types to our IndexConfig
    #[test]
    fn test_special_indexes_to_config_conversion() {
        // Helper function to create IndexModel
        fn create_test_index_model(key_name: &str, key_type: &str, index_name: &str) -> IndexModel {
            let mut keys = bson::Document::new();
            keys.insert(key_name.to_string(), bson::Bson::String(key_type.to_string()));

            let mut options = IndexOptions::default();
            options.name = Some(index_name.to_string());

            let mut model = IndexModel::builder().keys(keys).build();
            model.options = Some(options);
            model
        }

        // 1. Test 2dsphere index
        let geo_model = create_test_index_model("location", "2dsphere", "idx_geo_location");
        let geo_config = IndexConfig::from(geo_model);
        assert_eq!(geo_config.index_type, IndexType::Geo2DSphere, "Type should be Geo2DSphere");

        // 2. Test text index with language options
        let mut text_keys = bson::Document::new();
        text_keys.insert("description".to_string(), bson::Bson::String("text".to_string()));

        let mut text_options = IndexOptions::default();
        text_options.name = Some("idx_text_description".to_string());
        text_options.default_language = Some("english".to_string());

        let mut text_model = IndexModel::builder().keys(text_keys).build();
        text_model.options = Some(text_options);

        let text_config = IndexConfig::from(text_model);
        assert_eq!(text_config.index_type, IndexType::Text, "Type should be Text");
        assert_eq!(text_config.options["default_language"], serde_json::Value::String("english".to_string()),
                   "Should have English as default language");

        // 3. Test hashed index
        let hashed_model = create_test_index_model("userId", "hashed", "idx_hashed_userid");
        let hashed_config = IndexConfig::from(hashed_model);
        assert_eq!(hashed_config.index_type, IndexType::Hashed, "Type should be Hashed");
    }
}
