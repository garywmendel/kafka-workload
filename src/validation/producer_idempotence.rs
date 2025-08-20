use crate::domain::{
    MessageMetadata, TestEvent, TestLogLine, TestLogLocation, TestValidator, ValidationFailure
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ProducerIdempotenceValidator verifies that a message produced by a producer will present at only one topic / partition / offset
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProducerIdempotenceValidator {
    messages:
        BTreeMap<String, TestLogLocation<MessageMetadata>>,
}

impl TestValidator for ProducerIdempotenceValidator {
    fn validator_name(&self) -> &'static str {
        "producer-idempotence"
    }

    fn validate_event(&mut self, log: &TestLogLine) -> Vec<Result<(), ValidationFailure>> {
        match &log.data.fields {
            TestEvent::MessageReadSucceeded(event) => {
                if let Some(existing_metadata) = self.messages.get(&event.message.data.payload) {
                    if existing_metadata.data != event.message.metadata {
                        return vec![Err(ValidationFailure{
                            code: String::from("non-idempotent-message-read"),
                            line: log.line,
                            error: format!("{}, {}: identical message previously seen at {}", 
                                    event.consumer, event.message, existing_metadata.location())
                        })];
                    }
                } else {
                    self.messages.insert(event.message.data.payload.clone(), log.capture(event.message.metadata.clone()));
                }
            }
            TestEvent::MessageWriteSucceeded(event) => {
                if let Some(existing_metadata) = self.messages.get(&event.message.data.payload) {
                    if existing_metadata.data != event.message.metadata {
                        return vec![Err(ValidationFailure{
                            code: String::from("non-idempotent-message-written"),
                            line: log.line,
                            error: format!("{}, {}: identical message previously seen at {}", 
                                    event.producer, event.message, existing_metadata.location())
                        })];
                    }
                } else {
                    self.messages.insert(event.message.data.payload.clone(), log.capture(event.message.metadata.clone()));
                }
            }
            _ => {}
        }

        vec![Ok(())]
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: ProducerIdempotenceValidator = serde_json::from_str(data)?;
        self.messages = instance.messages;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
