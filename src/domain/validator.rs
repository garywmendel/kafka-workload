use super::TestLogLine;
use anyhow::Result;

pub struct ValidationFailure {
    pub line: u64,
    pub code: String,
    pub error: String,
}

pub trait TestValidator {
    fn validator_name(&self) -> &'static str;
    fn validate_event(&mut self, log: &TestLogLine) -> Vec<Result<(), ValidationFailure>>;
    fn load_state(&mut self, data: &str) -> Result<()> ;
    fn save_state(&self) -> Result<String>;
}