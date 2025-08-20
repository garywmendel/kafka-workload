use crate::domain::{
    TestEvent, TestLogLine, TestLogLocation, TestValidator, TopicName, TopicPartitionIndex,
    TopicPartitionOffset, ValidationFailure,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap, hash_map::DefaultHasher},
    hash::{Hasher, Hash}
};

/// MessageDurabilityValidatorMessage verifies that no message to modified between read and write, and that all messages are seen
/// at least once at the end of the test
/// TODO:
/// - without acks all some messages are lost
/// - without idempotent producers some messages are duplicated
type MessageReadHash = u64;
type MessageWriteHash = u64;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MessageIntegrityValidator {
    messages: BTreeMap<
        TopicName,
        BTreeMap<
            TopicPartitionIndex,
            BTreeMap<
                TopicPartitionOffset,
                (
                    Option<TestLogLocation<MessageReadHash>>,
                    Option<TestLogLocation<MessageWriteHash>>,
                ),
            >,
        >,
    >,
}

impl TestValidator for MessageIntegrityValidator {
    fn validator_name(&self) -> &'static str {
        "message-integrity"
    }

    fn validate_event(&mut self, log: &TestLogLine) -> Vec<Result<(), ValidationFailure>> {
        match &log.data.fields {
            TestEvent::MessageReadSucceeded(event) => {
                let mut hasher = DefaultHasher::new();
                event.message.data.hash(&mut hasher);
                let message_read_hash = hasher.finish();
                let existing_message_hashes = self
                    .messages
                    .entry(event.message.metadata.topic_name.clone())
                    .or_default()
                    .entry(event.message.metadata.topic_partition)
                    .or_default()
                    .entry(event.message.metadata.topic_partition_offset);

                match existing_message_hashes {
                    Entry::Vacant(entry) => {
                        entry.insert((Some(log.capture(message_read_hash)), None));
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        match &entry.0 {
                            Some(old_read_hash) => {
                                if old_read_hash.data != message_read_hash {
                                    return vec![Err(ValidationFailure {
                                        code: String::from("different-reads"),
                                        line: log.line,
                                        error: format!(
                                            "{}, {}: message data differs from previous read at {}",
                                            event.consumer,
                                            event.message,
                                            old_read_hash.location()
                                        ),
                                    })];
                                }
                            }
                            None => {
                                entry.0 = Some(log.capture(message_read_hash));
                            }
                        }
                    }
                }
                if let Some((Some(final_read_hash), Some(final_write_hash))) = self
                    .messages
                    .get(&event.message.metadata.topic_name)
                    .and_then(|partition_offsets| {
                        partition_offsets.get(&event.message.metadata.topic_partition)
                    })
                    .and_then(|offsets| offsets.get(&event.message.metadata.topic_partition_offset))
                {
                    if final_read_hash.data != final_write_hash.data {
                        return vec![Err(ValidationFailure{
                                code: String::from("write-read-different"),
                                line: log.line,
                                error: format!("{}, {}: message data differs from previous write by the producer at {}", event.consumer, event.message, final_write_hash.location())
                            })];
                    }
                }
            }
            TestEvent::MessageWriteSucceeded(event) => {
                let mut hasher = DefaultHasher::new();
                event.message.data.hash(&mut hasher);
                let message_write_hash = hasher.finish();
                let existing_message_hashes = self
                    .messages
                    .entry(event.message.metadata.topic_name.clone())
                    .or_default()
                    .entry(event.message.metadata.topic_partition)
                    .or_default()
                    .entry(event.message.metadata.topic_partition_offset);

                match existing_message_hashes {
                    Entry::Vacant(entry) => {
                        entry.insert((None, Some(log.capture(message_write_hash))));
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        match &entry.1 {
                            Some(old_write_hash) => {
                                if old_write_hash.data != message_write_hash {
                                    return vec![Err(ValidationFailure{
                                        code: String::from("different-writes"),
                                        line: log.line,
                                        error: format!("{}, {}: message data differs from previous write at {}", 
                                                event.producer, event.message,old_write_hash.location())
                                    })];
                                }
                            }
                            None => {
                                entry.1 = Some(log.capture(message_write_hash));
                            }
                        }
                    }
                }
                if let Some((Some(final_read_hash), Some(final_write_hash))) = self
                    .messages
                    .get(&event.message.metadata.topic_name)
                    .and_then(|partition_offsets| {
                        partition_offsets.get(&event.message.metadata.topic_partition)
                    })
                    .and_then(|offsets| offsets.get(&event.message.metadata.topic_partition_offset))
                {
                    if final_read_hash.data != final_write_hash.data {
                        return vec![Err(ValidationFailure{
                                code: String::from("read-write-different"),
                                line: log.line,
                                error: format!("{}, {}: message data differs from previous read by the consumer at {}", event.producer, event.message, final_read_hash.location())
                            })];
                    }
                }
            }
            TestEvent::WorkloadEnded => {
                let mut results = Vec::new();
                for (topic_name, partition_offsets) in self.messages.iter() {
                    for (topic_partition, topic_partition_offset, read_write_state) in
                        partition_offsets
                            .iter()
                            .flat_map(|(topic_partition, offsets)| {
                                offsets
                                    .iter()
                                    .map(move |(offset, state)| (topic_partition, offset, state))
                            })
                    {
                        match read_write_state {
                            (Some(_), Some(_)) => {
                                // nothing to do here, since we already incrementally validated this
                            }
                            (Some(message_read_hash), None) => {
                                results.push(Err(ValidationFailure {
                                        code: String::from("read-never-written"),
                                        line: message_read_hash.line,
                                        error: format!(
                                            "topic = '{}', partition = {}, offset = {}: message read at {} was never written",
                                            topic_name,
                                            topic_partition,
                                            topic_partition_offset,
                                            message_read_hash.location()
                                        ),
                                    }));
                            }
                            (None, Some(message_write_hash)) => {
                                results.push(Err(ValidationFailure {
                                        code: String::from("write-never-read"),
                                        line: message_write_hash.line,
                                        error: format!(
                                            "topic = '{}', partition = {}, offset = {}: message write at {} was never read by any consumer, it may be lost",
                                            topic_name,
                                            topic_partition,
                                            topic_partition_offset,
                                            message_write_hash.location()
                                        ),
                                    }));
                            }
                            (None, None) => {
                                unreachable!()
                            }
                        }
                    }
                }
                return results;
            }
            _ => {}
        }
        vec![Ok(())]
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: MessageIntegrityValidator = serde_json::from_str(data)?;
        self.messages = instance.messages;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
