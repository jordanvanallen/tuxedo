/*!
 * MongoDB Index Conversion Module
 * 
 * This module handles bidirectional conversions between our internal Tuxedo index 
 * representations and MongoDB's native index types.
 * 
 * Key features:
 * - Converts our IndexConfig to MongoDB's IndexModel for creating indexes
 * - Converts MongoDB's IndexModel back to our IndexConfig for analysis
 * - Handles special index types (text, geo, hashed)
 * - Supports bulk conversions of multiple indexes
 * 
 * These conversions enable us to seamlessly move index definitions between 
 * source and destination databases during data migrations.
 */

use crate::database::index::{IndexConfig, IndexField, IndexType, SourceIndexes};
use mongodb::{bson::Document, options::IndexOptions, IndexModel};
use std::collections::HashMap;

/// Converts a MongoDB IndexModel to our internal IndexConfig format
/// 
/// This allows us to read indexes from a MongoDB source and convert
/// them to our standardized format for cross-database operations.
impl From<IndexModel> for IndexConfig {
    fn from(model: IndexModel) -> Self {
        // Extract fields from keys
        let fields: Vec<IndexField> = model.keys.iter()
            .map(|(name, value)| {
                let direction = match value {
                    mongodb::bson::Bson::Int32(1) | mongodb::bson::Bson::Int64(1) =>
                        crate::database::index::IndexDirection::Ascending,
                    mongodb::bson::Bson::Int32(-1) | mongodb::bson::Bson::Int64(-1) =>
                        crate::database::index::IndexDirection::Descending,
                    _ => crate::database::index::IndexDirection::Ascending,
                };

                IndexField {
                    name: name.to_string(),
                    direction,
                }
            })
            .collect();

        // Determine index type based on options and key values
        let mut index_type = IndexType::Standard;
        let mut options_map = HashMap::new();

        if let Some(opts) = model.options {
            // Check for unique index
            if let Some(true) = opts.unique {
                index_type = IndexType::Unique;
                options_map.insert("unique".to_string(), serde_json::Value::Bool(true));
            }

            // Check for text index
            if model.keys.values().any(|v| (v.as_str() == Some("text"))) {
                index_type = IndexType::Text;

                // Add language options if present
                if let Some(lang) = opts.default_language {
                    options_map.insert("default_language".to_string(), serde_json::Value::String(lang));
                }

                if let Some(lang_override) = opts.language_override {
                    options_map.insert("language_override".to_string(), serde_json::Value::String(lang_override));
                }
            }

            // Check for 2dsphere index
            if model.keys.values().any(|v| (v.as_str() == Some("2dsphere"))) {
                index_type = IndexType::Geo2DSphere;
            }

            // Check for 2d index
            if model.keys.values().any(|v| (v.as_str() == Some("2d"))) {
                index_type = IndexType::Geo2D;
            }

            // Check for hashed index
            if model.keys.values().any(|v| (v.as_str() == Some("hashed"))) {
                index_type = IndexType::Hashed;
            }

            // Add sparse option if present
            if let Some(sparse) = opts.sparse {
                options_map.insert("sparse".to_string(), serde_json::Value::Bool(sparse));
            }

            IndexConfig {
                name: opts.name.unwrap_or_else(|| "unnamed_index".to_string()),
                fields,
                index_type,
                options: options_map,
            }
        } else {
            // If no options, create a standard index with default name
            IndexConfig {
                name: "unnamed_index".to_string(),
                fields,
                index_type,
                options: options_map,
            }
        }
    }
}

/// Converts our internal IndexConfig to MongoDB's IndexModel
/// 
/// This allows us to easily create indexes in MongoDB using our
/// standardized index definitions.
impl From<&IndexConfig> for IndexModel {
    fn from(config: &IndexConfig) -> Self {
        // Create the keys document
        let mut keys = Document::new();
        for field in &config.fields {
            keys.insert(field.name.clone(), bson::Bson::from(&field.direction));
        }

        // Create the appropriate index options based on the index type
        let mut options = IndexOptions::default();
        options.name = Some(config.name.clone());

        // Set appropriate options based on index type
        match config.index_type {
            IndexType::Unique => {
                options.unique = Some(true);
            }
            IndexType::Text => {
                // For text indexes, MongoDB expects the value to be "text"
                // We'll modify the existing keys document for text fields
                for field in &config.fields {
                    // Replace the standard 1/-1 direction value with "text" string
                    keys.insert(field.name.clone(), bson::Bson::String("text".to_string()));
                }

                // Add text-specific options if needed
                if let Some(value) = config.options.get("default_language") {
                    if let Some(lang) = value.as_str() {
                        options.default_language = Some(lang.to_string());
                    }
                }

                if let Some(value) = config.options.get("language_override") {
                    if let Some(lang_override) = value.as_str() {
                        options.language_override = Some(lang_override.to_string());
                    }
                }
            }
            IndexType::Geo2DSphere => {
                // For 2dsphere indexes, we need to use a special value in the keys document
                // The keys have already been populated above, but MongoDB expects
                // the value to be "2dsphere" for geospatial indexes.
                // We'll modify the existing keys document for geo fields
                for field in &config.fields {
                    // Replace the standard 1/-1 direction value with "2dsphere" string
                    keys.insert(field.name.clone(), bson::Bson::String("2dsphere".to_string()));
                }
            }
            IndexType::Geo2D => {
                // For 2d indexes, MongoDB expects the value to be "2d"
                for field in &config.fields {
                    // Replace the standard 1/-1 direction value with "2d" string
                    keys.insert(field.name.clone(), bson::Bson::String("2d".to_string()));
                }
            }
            IndexType::Hashed => {
                // For hashed indexes, MongoDB expects the value to be "hashed"
                for field in &config.fields {
                    // Replace the standard 1/-1 direction value with "hashed" string
                    keys.insert(field.name.clone(), bson::Bson::String("hashed".to_string()));
                }
            }
            _ => {}
        }

        // Add sparse option if present
        if let Some(value) = config.options.get("sparse") {
            if let Some(sparse) = value.as_bool() {
                options.sparse = Some(sparse);
            }
        }

        // Use the typed builder pattern to create the IndexModel
        let mut index_model = mongodb::IndexModel::builder().keys(keys).build();
        index_model.options = Some(options);
        index_model
    }
}

/// Converts a collection of index configs to MongoDB index models
/// 
/// This is a utility conversion that makes it easy to convert all indexes
/// for a collection at once to create them in MongoDB.
impl From<SourceIndexes> for Vec<IndexModel> {
    fn from(source_indexes: SourceIndexes) -> Self {
        source_indexes.indexes
            .iter()
            .map(IndexModel::from)
            .collect()
    }
}

/// Creates source indexes from a list of MongoDB index models
/// 
/// This is useful when reading all indexes from a MongoDB collection
/// and converting them to our internal format.
impl From<(Vec<IndexModel>, String)> for SourceIndexes {
    fn from((models, entity_name): (Vec<IndexModel>, String)) -> Self {
        let indexes = models
            .into_iter()
            .filter(|model| model.keys.get("_id").is_none())
            .map(IndexConfig::from)
            .collect();

        SourceIndexes {
            entity_name,
            indexes,
        }
    }
} 