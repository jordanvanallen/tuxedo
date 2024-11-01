use std::future::Future;

use crate::{MongoDbModelError, Result};

use bson::oid::ObjectId;
use bson::{doc, Bson, Document};
use mongodb::options::{
    Acknowledgment, CollectionOptions, DeleteOptions, FindOneAndDeleteOptions, FindOneOptions,
    FindOptions, InsertOneOptions, ReadConcern, ReplaceOptions, WriteConcern,
};
use mongodb::results::InsertOneResult;
use mongodb::{Collection, Cursor, Database};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait MongoModel
where
    Self: Clone + Serialize + DeserializeOwned + Unpin + Send + Sync,
{
    /// Defines the collection name that backs models of this type.
    ///
    /// All MongoModel instances need to annotate the expected collection that they read data from and send data to.
    /// This annotation allows us to set this part of the underlying MongoDB Driver calls during compile time, since
    /// that should never really change while the program is running.
    const COLLECTION_NAME: &'static str;

    /// The expected default read concern for database communication.
    fn read_concern() -> ReadConcern {
        ReadConcern::majority()
    }

    /// The expected default write concern for database communication.
    fn write_concern() -> WriteConcern {
        WriteConcern::builder()
            .w(Some(Acknowledgment::Majority))
            .journal(Some(true))
            .build()
    }

    /// Convenience to quickly get a handle to the driver's Collection object for the underlying
    /// database collection in MongoDB.
    ///
    /// Internal functions use this to abstract away common settings for read/write concerns.
    /// Application code can use this to directly call into the underying driver if they want to
    /// easily make calls directly to the database without having to set up the preamble themselves.
    fn collection(db: &Database) -> Collection<Self> {
        let options = CollectionOptions::builder()
            .read_concern(Self::read_concern())
            .write_concern(Self::write_concern())
            .build();

        db.collection_with_options(Self::COLLECTION_NAME, options)
    }

    /// Maps the struct field representing the ID of this document.
    fn id(&self) -> Option<ObjectId>;

    /// Sets the ID field on the model instance. Primarily used when saving a new document, since
    /// users in most cases will not be setting an ID themselves and will expect us to handle that
    /// as part of saving the data.
    fn set_id(&mut self, id: ObjectId);

    /// Saves a model struct to the backing database.
    ///
    /// We return the driver Result which encapsulates any underlying errors that may have arisen during saving.
    /// Note that the UpdateResult is just metadata about the successful write -- it says nothing about the actual data
    /// that was saved to the database. To get that you can either subsequently run the find method and pass in the ID
    /// from the same struct instance you passed in, or simply read data out of the struct itself.
    fn save(&mut self, db: &Database) -> impl Future<Output = Result<()>> + Send {
        async {
            // Ensure we enable journaling, regardless of what the model is configured to actually use.
            let mut write_concern = Self::write_concern();
            write_concern.journal = Some(true);

            match self.id() {
                // If the document has an ID already, assume this is an update.
                Some(ref id) => {
                    let doc = doc! { "_id": id };
                    let opts = ReplaceOptions::builder()
                        .write_concern(write_concern)
                        .build();
                    Self::collection(db)
                        .replace_one(doc, self)
                        .with_options(opts)
                        .await?;
                    Ok(())
                }
                // If the document does not have an ID, this is an insert.
                None => {
                    let opts = InsertOneOptions::builder()
                        .write_concern(write_concern)
                        .build();

                    let InsertOneResult {
                        inserted_id: id, ..
                    } = Self::collection(db)
                        .insert_one(&*self)
                        .with_options(opts)
                        .await?;

                    match id {
                        Bson::ObjectId(oid) => self.set_id(oid),
                        _ => return Err(MongoDbModelError::MongoDBInvalidIdTypeAfterInsert),
                    };

                    Ok(())
                }
            }
        }
    }

    /// Removes the instance's document from the database.
    ///
    /// Returns an error in the following cases:
    /// - if the document was never persisted
    /// - if the driver encountered an error deleting the document for some reason.
    fn delete(&self, db: &Database) -> impl Future<Output = Result<()>> {
        self.delete_with_options(db, None)
    }

    /// Removes the instance's related document from the database.
    ///
    /// Functionally equivalent to +delete(db)+, but allows you to override the MongoDB driver
    /// options.
    fn delete_with_options<O>(
        &self,
        db: &Database,
        options: O,
    ) -> impl Future<Output = Result<()>> + Send
    where
        O: Into<Option<DeleteOptions>> + Send,
    {
        async {
            let id = self.id().ok_or(MongoDbModelError::ModelIdMissingOnDelete)?;
            let query = doc! { "_id": id };
            let _delete_result = Self::collection(db)
                .delete_one(query.into())
                .with_options(options)
                .await?;
            Ok(())
        }
    }

    /// Deletes the associated document from the database, and returns a new model instance
    /// representing the deleted data.
    fn delete_and_return(self, db: &Database) -> impl Future<Output = Result<Option<Self>>> + Send {
        self.delete_and_return_with_options(db, None)
    }

    /// Deletes the associated document from the database, and returns a new model instance
    /// representing the deleted data.
    ///
    /// Functionally identical to +delete_and_return(db)+, but allows you to override the options
    /// provided to the mongodb driver.
    fn delete_and_return_with_options<O>(
        self,
        db: &Database,
        options: O,
    ) -> impl Future<Output = Result<Option<Self>>> + Send
    where
        O: Into<Option<FindOneAndDeleteOptions>> + Send,
    {
        async move {
            // If the current model does not have an ID, we can just drop the value with a no-op.
            let id = match self.id() {
                Some(v) => v,
                None => return Ok(None),
            };

            let query = doc! { "_id": id };

            Ok(Self::collection(db)
                .find_one_and_delete(query)
                .with_options(options)
                .await?)
        }
    }

    /// Deletes all documents in the model's collection that match the provided filter/query.
    fn delete_all<D>(db: &Database, query: D) -> impl Future<Output = Result<()>> + Send
    where
        D: Into<Document> + Send + 'static,
    {
        Self::delete_all_with_options(db, query.into(), None)
    }

    /// Deletes all documents in the model's collection that match the provided filter/query.
    ///
    /// Functionally identical to +delete_many(db, query)+, but allows you to override the MongoDB
    /// driver options.
    fn delete_all_with_options<D, O>(
        db: &Database,
        query: D,
        options: O,
    ) -> impl Future<Output = Result<()>> + Send
    where
        D: Into<Document> + Send + 'static,
        O: Into<Option<DeleteOptions>> + Send + 'static,
    {
        async {
            let _delete_result = Self::collection(db)
                .delete_many(query.into())
                .with_options(options)
                .await?;
            Ok(())
        }
    }

    /// Finds a given model instance in the database using the primary ID field.
    ///
    /// - If the is is Ok(Some(T)), the resource we found is the inner T value
    /// - If the Result is Ok(None) then the query was accepted by the database but nothing was returned.
    /// - If the result was Err, then you will have to look at the error's value to determine what the issue is.
    fn find<I>(db: &Database, id: I) -> impl Future<Output = Result<Self>> + Send
    where
        I: Into<ObjectId>,
    {
        Self::find_with_options(&db, id.into(), None)
    }

    /// Finds a given model instance in the database using the primary ID field and a provided set
    /// of driver options.
    ///
    /// Functionally identical to +find(db, id)+ but with the ability to define specific database
    /// driver options to the underlying mongodb driver.
    fn find_with_options<I, O>(
        db: &Database,
        id: I,
        options: O,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        I: Into<ObjectId> + Send,
        O: Into<Option<FindOneOptions>> + Send,
    {
        async move {
            let query = doc! { "_id": id.into() };
            Self::collection(db)
                .find_one(query.clone())
                .with_options(options)
                .await?
                .ok_or(MongoDbModelError::RecordNotFound {
                    collection: Self::COLLECTION_NAME,
                    query,
                })
        }
    }

    /// Like find, but with a custom query to use to find a record for instead of simply an ID.
    ///
    /// Will only return the FIRST object that matches this query. If you want all objects, you should consider the
    /// where method instead.
    fn find_by<D>(db: &Database, query: D) -> impl Future<Output = Result<Self>> + Send
    where
        D: Into<Option<Document>> + Send,
    {
        Self::find_by_with_options(db, query, None)
    }

    /// Identify a unique document matching the given query/filter, applying the provided "find
    /// options".
    ///
    /// Functionally equivalent to +find_by(db, query)+ with the ability to override the default
    /// options passed to the underlying mongodb database driver.
    fn find_by_with_options<D, O>(
        db: &Database,
        query: D,
        options: O,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        D: Into<Option<Document>> + Send,
        O: Into<Option<FindOneOptions>> + Send,
    {
        async {
            let q: bson::Document = query.into().unwrap_or_default();
            Self::collection(db)
                .find_one(q.clone())
                .with_options(options)
                .await?
                .ok_or(MongoDbModelError::RecordNotFound {
                    collection: Self::COLLECTION_NAME,
                    query: q,
                })
        }
    }

    /// Perform a search to find all documents that match the input query.
    ///
    /// Like find_by, but returns a Cursor that looks at +all+ documents that match the condition. Typically the
    /// cursor is converted into a list with +try_collect::<Vec<Self>>()+.
    fn find_all<D>(db: &Database, query: D) -> impl Future<Output = Result<Cursor<Self>>> + Send
    where
        D: Into<Document> + Send,
    {
        Self::find_all_with_options(db, query, None)
    }

    /// Find all documents matching the given query/filter, applying the provided "find options".
    ///
    /// Functionally equivalent to +find_all(dq, query)+ with the ability to override the default
    /// options passed to the underlying mongodb database driver.
    fn find_all_with_options<D, O>(
        db: &Database,
        query: D,
        options: O,
    ) -> impl Future<Output = Result<Cursor<Self>>> + Send
    where
        D: Into<Document> + Send,
        O: Into<Option<FindOptions>> + Send,
    {
        async move {
            db.collection(Self::COLLECTION_NAME)
                .find(query.into())
                .with_options(options)
                .await
                .map_err(Into::into)
        }
    }

    /// Sync fields with the database, overwriting any changes in the current struct.
    ///
    /// Destroys +self+ and pushes out the refreshed data from the database. This is effectively a
    /// convenience around having to run find with the ID yourself.
    fn sync(&mut self, db: &Database) -> impl std::future::Future<Output = Result<Self>> + Send {
        async {
            // First we need to find the ID so we can do a refetch from the DB. If we don't have an ID,
            // then we never saved and thus syncing data is useless here because theres no way to query
            // for new data (and in all liklihood it doesn't exist).
            let id = match self.id() {
                Some(v) => v,
                None => return Ok(self.clone()),
            };

            // Force syncing to fail if the document was deleted externally.
            let record = Self::find(db, id.clone()).await?;

            // Update Self with the new data, and return the old self data in case users need to compare.

            let old_data = self.clone();
            *self = record;

            Ok(old_data)
        }
    }
}
