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

    pub async fn build(self) -> TuxedoResult<MongodbSource> {
        let database_name = self
            .database_name
            .ok_or_else(||
                TuxedoError::Generic("No database name provided for mongodb source database".into())
            )?;
        let client_options = self
            .client_options
            .ok_or_else(||
                TuxedoError::Generic("No client options provided for mongodb source database".into())
            )?;

        // TODO: Should we be helping here set things like pool sizes based on threads?
        let client = Client::with_options(client_options.into())?;
        let db = client.database(database_name.as_str());
        let read_options = self.read_options.unwrap_or_else(|| Default::default());

        MongodbSource::new(
            client,
            db,
            read_options,
            self.count_options,
        ).await
    }
}