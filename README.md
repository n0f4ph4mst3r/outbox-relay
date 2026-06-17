# Outbox Relay

Lightweight Go service that implements the Transactional Outbox pattern. It ensures reliable event delivery from a database to a message broker, guaranteeing "at-least-once" message delivery.

## System requirements

You need to have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed in order to build and run the project. No additional tools required.

## How to run with Docker

Define environment variables. You can copy environment from [example](https://github.com/n0f4ph4mst3r/outbox-relay/blob/main/.env.sample)

    cp .env.sample .env

Perform

	sudo docker-compose up 

The service will start according to your configuration.

## How to run manually

### Tools

To develop the app manually, you need the following tools installed:

- [Go](https://go.dev/) (version 1.26.4 or newer)
- [PostgreSQL](https://www.postgresql.org/) database
- [Kafka](https://kafka.apache.org/) broker

### Infrastructure Setup

If you don't have Postgres or Kafka installed natively on your machine, you can spin them up quickly inside Docker containers using the provided Compose file:

    sudo docker-compose up -d postgres db-migrator redpanda topic-init

*Note*: This command starts the infrastructure components in the background (`-d`). Wait until all containers are healthy before running the application. You can check their status using `docker ps`


## Usage

To trigger an event, run next SQL query:
    
``` sql
    INSERT INTO outbox_events (event_type, payload) 
    VALUES ('OutboxEvent', '{"param1": "...", "param2": "..."}');
```

## Deployment

It is recommended to run the worker as a sidecar container or a separate service within the same network as the main application to minimize latency and ensure database proximity.