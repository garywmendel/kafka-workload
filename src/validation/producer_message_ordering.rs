use crate::domain::{
    TestEvent, TestLogLine, TestLogLocation, TestValidator, TopicName, TopicPartitionIndex,
    TopicPartitionOffset, ValidationFailure,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ProducerMessageOrderingValidator verifies that a producer will produce sequential messages on sequential offsets given:
/// - producer is idempotent
/// - producer has acks all
/// - producer retries infinitely on failure
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProducerMessageOrderingValidator {
    producer_topic_partition_offsets: BTreeMap<
        String,
        BTreeMap<TopicName, BTreeMap<TopicPartitionIndex, TestLogLocation<TopicPartitionOffset>>>,
    >,
}

impl TestValidator for ProducerMessageOrderingValidator {
    fn validator_name(&self) -> &'static str {
        "producer-message-ordering"
    }

    fn validate_event(&mut self, log: &TestLogLine) -> Vec<Result<(), ValidationFailure>> {
        if let TestEvent::MessageWriteSucceeded(event) = &log.data.fields {
            if let Some(last_offset) = self
                .producer_topic_partition_offsets
                .get_mut(&event.producer.id)
                .and_then(|topics| topics.get_mut(&event.message.metadata.topic_name))
                .and_then(|partitions| {
                    partitions.get_mut(&event.message.metadata.topic_partition)
                })
            {
                if last_offset.data > event.message.metadata.topic_partition_offset {
                    let bad_offset = last_offset.clone();
                    *last_offset = log.capture(event.message.metadata.topic_partition_offset);
                    return vec![Err(ValidationFailure {
                        code: String::from("non-monotonic"),
                        line: log.line,
                        error: format!("{}, {}: message offset is not greater than the previous offset {} at {}", 
                                event.producer,
                                event.message,
                                bad_offset.data,
                                bad_offset.location()
                                )
                    })];
                } else {
                    *last_offset = log.capture(event.message.metadata.topic_partition_offset);
                }
            } else {
                self.producer_topic_partition_offsets
                    .entry(event.producer.id.clone())
                    .or_default()
                    .entry(event.message.metadata.topic_name.to_string())
                    .or_default()
                    .insert(
                        event.message.metadata.topic_partition,
                        log.capture(event.message.metadata.topic_partition_offset),
                    );
            };
        }
        vec![Ok(())]
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: ProducerMessageOrderingValidator = serde_json::from_str(data)?;
        self.producer_topic_partition_offsets = instance.producer_topic_partition_offsets;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
