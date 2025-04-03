use super::MongodbDestination;
use crate::{TuxedoError, TuxedoResult};
use mongodb::options::{ClientOptions, InsertManyOptions};
use mongodb::Client;

#[derive(Default)]
pub struct MongodbDestinationBuilder {
    database_name: Option<String>,
    client_options: Option<ClientOptions>,
    write_options: Option<InsertManyOptions>,
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

    pub async fn build(self) -> TuxedoResult<MongodbDestination> {
        let database_name = self
            .database_name
            .ok_or_else(||
                TuxedoError::Generic("No database name provided for mongodb destination database".into())
            )?;
        let client_options = self
            .client_options
            .ok_or_else(||
                TuxedoError::Generic("No client options provided for mongodb destination database".into())
            )?;

        // TODO: Should we be helping here set things like pool sizes based on threads?
        let client = Client::with_options(client_options.into())?;
        let db = client.database(database_name.as_str());

        Ok(MongodbDestination::new(
            client,
            db,
            self.write_options,
        ))
    }
}