use crate::database::traits::{Destination, Source};

#[derive(Debug)]
pub(crate) struct DatabasePair<S, D>
where
    S: Source,
    D: Destination,
{
    source: S,
    destination: D,
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
