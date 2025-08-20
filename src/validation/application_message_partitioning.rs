use crate::domain::{
    TestEvent, TestLogLine, TestLogLocation, TestValidator, TopicName, TopicPartitionIndex,
    ValidationFailure,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ApplicationMessagePartitioningValidator verifies that the same partition is always assigned to the same key on the same topic
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ApplicationMessagePartitioningValidator {
    application_topic_keys:
        BTreeMap<TopicName, BTreeMap<String, TestLogLocation<TopicPartitionIndex>>>,
}

impl TestValidator for ApplicationMessagePartitioningValidator {
    fn validator_name(&self) -> &'static str {
        "application-message-partitioning"
    }

    fn validate_event(&mut self, log: &TestLogLine) -> Vec<Result<(), ValidationFailure>> {
        if let TestEvent::MessageReadSucceeded(event) = &log.data.fields {
            // Validate this only if the key is present
            if event.message.data.key.is_some() {
                let topic_keys = self
                    .application_topic_keys
                    .entry(event.message.metadata.topic_name.clone())
                    .or_default();

                let message_key = event.message.data.key.as_ref().unwrap().clone();
                match topic_keys.get(&message_key) {
                    Some(existing_partition_assignment) => {
                        if existing_partition_assignment.data
                            != event.message.metadata.topic_partition
                        {
                            return vec![Err(ValidationFailure { 
                                code: String::from("key-reassigned"),
                                line: log.line, 
                                error: format!(
                                "{}, {}: message key was previously assigned to partition {} at {}",
                                event.consumer, event.message, existing_partition_assignment.data, existing_partition_assignment.location()
                            ), })];
                        }
                    }
                    None => {
                        topic_keys.insert(
                            message_key,
                            log.capture(event.message.metadata.topic_partition),
                        );
                    }
                }
            }
        }
        vec![Ok(())]
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: ApplicationMessagePartitioningValidator = serde_json::from_str(data)?;
        self.application_topic_keys = instance.application_topic_keys;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
