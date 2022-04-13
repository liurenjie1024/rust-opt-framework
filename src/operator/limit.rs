#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Limit {
    limit: usize,
}

impl Limit {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}
