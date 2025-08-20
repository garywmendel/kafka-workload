FROM docker.io/rust:1.73 as builder
WORKDIR /app
RUN apt-get update && apt-get install -y cmake
COPY src /app/src
COPY Cargo.toml /app/Cargo.toml
COPY Cargo.lock /app/Cargo.lock
RUN cargo install --locked --path .

FROM docker.io/debian:bookworm-slim
# RUN apt-get update && apt-get install -y extra-runtime-dependencies && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/workload /app/workload
COPY --from=builder /usr/local/cargo/bin/validation /app/validation
COPY --from=builder /usr/local/cargo/bin/load /app/load
COPY config-docker.json /app/workload-config.json
COPY scripts/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
RUN mkdir -p /app/logs
WORKDIR /app
CMD ["/app/entrypoint.sh"]


# FROM docker.io/rust:1.73 as builder
# WORKDIR /app
# RUN apt-get update && apt-get install -y cmake
# COPY workload/src /app/src
# COPY workload/Cargo.toml /app/Cargo.toml
# COPY workload/Cargo.lock /app/Cargo.lock
# RUN cargo install --locked --path .



# FROM ubuntu:latest AS final
# COPY --from=ar-kafka-workload:latest / /
# RUN apt-get update -y && apt-get install -y curl jq
