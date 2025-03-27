use crate::database::traits::{Destination, Source};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DatabasePair<S, D>
where
    S: Source,
    D: Destination,
{
    pub(crate) source: S,
    pub(crate) destination: D,
}

impl<S, D> DatabasePair<S, D>
where
    S: Source,
    D: Destination,
{
    pub(crate) fn new(source: S, destination: D) -> Self {
        Self {
            source,
            destination,
        }
    }
}
