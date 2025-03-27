#[derive(Debug, Clone)]
pub struct PaginationOptions {
    /// The position from which to start retrieving records
    /// Equivalent to 'skip' In MongoDB, 'offset' in Postgres
    pub start_position: u64,

    /// Maximum number of records to retrieve from the database
    pub limit: u64,
}

impl PaginationOptions {
    pub fn new(start_position: u64, limit: u64) -> PaginationOptions {
        PaginationOptions { start_position, limit }
    }
}