# Kafka Workload

## Start the Kafka Cluster

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