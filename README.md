# Antithesis Kafka Workload

## Workload Objectives

This workload is intended for use with any system that uses the Apache Kafka protocol to verify common Kafka properties that should always hold true, regardless of environmental conditions. 

This workload uses the [rdkafka](https://crates.io/crates/rdkafka) client library; as of August 2025, it's using version [0.36.2](https://crates.io/crates/rdkafka/0.36.2), which is compatible with librdkafka v1.9.2+.

As of August 2025, this workload expects to run against a 3-node Kafka cluster. The cluster configuration (set via the producer) strives for the strongest consistency guarantees- the number of in-sync replicas and the replication factor are both 3, with the producer expected acknowledgements from all nodes to consider a write to be successful. 

## Workload Test Properties

As of August 2025, there are five properties being asserted via the Antithesis SDK: 

1. Messages don't change partitions.
2. Previously-committed consumer offsets are never seen again. 
3. There are no "message integrity" violations (specifically, there are no "lost" messages or messages that were read but never written). 
4. The producer doesn't "double-write" messages. 
5. Sequential messages are in sequential offsets. 

## How to Use (and Validate!)

This workload is intended to run in any environment with a Kafka-compatible cluster. Try it locally with your own Kafka-compatible system!

### Start the Kafka Cluster

```bash
podman-compose up -d
```

## Build the Workload Image

```bash
podman build . -t kafka-workload
```


## Start the Workload

```bash
podman run -it --net host --name workload -d kafka-workload
```