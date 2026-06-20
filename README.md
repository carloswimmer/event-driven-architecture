# Event-Driven Architecture

A hands-on study project that implements a payment and order fulfillment flow using **event-driven microservices**. The original design mirrors a GCP architecture (Cloud Tasks + Pub/Sub); locally it uses **RabbitMQ** for point-to-point commands and **Kafka** for pub/sub events.

Built to practice patterns used in production: explicit messaging topology as code, separate broker credentials, health checks, idempotent consumers, and Docker Compose overlays for dev vs base config.

## Architecture

```mermaid
flowchart LR
  Frontend -->|POST_startPayment| ApiGateway
  ApiGateway -->|SSE| Frontend
  ApiGateway -->|payment.requested| RabbitMQ
  RabbitMQ --> PaymentService
  PaymentService --> StripeMock
  StripeMock -->|webhook_success| PaymentService
  PaymentService -->|paymentSucceeded| Kafka
  Kafka --> ApiGateway
  Kafka --> AvailabilityService
  Kafka --> AnalyticsService
  Kafka --> InvoiceService
  InvoiceService -->|invoiceCreated| Kafka
  Kafka --> AnalyticsService
  Kafka --> NotificationService
  NotificationService --> SendGridMock
```

| GCP (reference diagram) | Local stack | Role |
|-------------------------|-------------|------|
| Cloud Tasks | **RabbitMQ** | Point-to-point: `payment.requested` → Payment Service |
| Pub/Sub | **Kafka** | Fan-out: `paymentSucceeded`, `invoiceCreated` |
| Stripe | `mocks/stripe-mock` | PaymentIntent + webhook |
| SendGrid | `mocks/sendgrid-mock` | Email delivery |

### Event flow

1. **API Gateway** receives `POST /orders`, reserves a product, publishes `payment.requested` to RabbitMQ.
2. **Payment Service** consumes the command, calls the Stripe mock, and publishes `paymentSucceeded` to Kafka on webhook success.
3. **Availability**, **Analytics**, **Invoice**, and **API Gateway** consume `paymentSucceeded` in parallel.
4. **Invoice Service** publishes `invoiceCreated` to Kafka.
5. **Analytics** and **Notification** consume `invoiceCreated`; Notification sends email via the SendGrid mock.
6. **API Gateway** pushes `payment_succeeded` to the client over SSE.

## Current status

| Layer | Status |
|-------|--------|
| RabbitMQ + Kafka infrastructure | Ready (`docker-compose.yml`, `infra/`) |
| Microservices (NestJS monorepo) | Follow [`.cursor/tutorial.md`](.cursor/tutorial.md) to implement |

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Node.js 20+](https://nodejs.org/) and npm 10+ (for microservices — after Phase 1 of the tutorial)
- `curl` (for HTTP/SSE testing)

## Quick start — messaging infrastructure

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and replace placeholder passwords before any non-local use.

### 2. Start RabbitMQ and Kafka

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

Use **both** compose files locally: the base file runs brokers on the internal network; the dev overlay exposes ports and adds Kafka UI.

### 3. Verify

| Service | URL |
|---------|-----|
| RabbitMQ Management | http://localhost:15672 (`admin` / value of `RABBITMQ_PASS`) |
| Kafka UI | http://localhost:8080 |

Expected RabbitMQ topology (vhost `eda`):

- Exchange `eda.commands` → queue `payment.requested` (quorum, with DLQ)
- Dead-letter exchange `eda.dlx` → queue `payment.requested.dlq`

Expected Kafka topics:

- `paymentSucceeded` (6 partitions)
- `invoiceCreated` (3 partitions)

Check init jobs completed successfully:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml ps
```

`eda-rabbitmq-init` and `eda-kafka-init` should show `Exited (0)`.

## Implementing the full system

Step-by-step instructions for the NestJS + Fastify monorepo, all microservices, mocks, and end-to-end testing live in:

**[`.cursor/tutorial.md`](.cursor/tutorial.md)**

Phases 0–13 cover infrastructure verification through production notes. Work through them in order; each phase includes copy-paste code, commands, and checkpoints.

### Target project layout (after tutorial)

```text
event-driven-architecture/
├── docker-compose.yml          # Base stack (infra + services)
├── docker-compose.dev.yml      # Local port mappings + Kafka UI
├── infra/                      # RabbitMQ/Kafka topology as code
├── packages/
│   ├── contracts/              # Event schemas, topics, routing keys
│   └── shared/                 # Health, idempotency, env helpers
├── services/
│   ├── api-gateway/
│   ├── payment/
│   ├── availability/
│   ├── analytics/
│   ├── invoice/
│   └── notification/
└── mocks/
    ├── stripe-mock/
    └── sendgrid-mock/
```

## Project structure (today)

```text
event-driven-architecture/
├── docker-compose.yml
├── docker-compose.dev.yml
├── .env.example
├── infra/
│   ├── rabbitmq/
│   │   ├── definitions.json    # Exchanges, queues, bindings
│   │   ├── init.sh             # Import topology + app user
│   │   ├── rabbitmq.conf
│   │   └── enabled_plugins
│   └── kafka/
│       └── init-topics.sh      # Explicit topic creation
└── .cursor/
    └── tutorial.md             # Full implementation guide
```

## Configuration

### Compose files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Production-oriented base: brokers, init jobs, resource limits, no public ports |
| `docker-compose.dev.yml` | Local overlay: exposes AMQP/Kafka ports, adds Kafka UI |

```bash
# Local development
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Base only (brokers internal to Docker network)
docker compose up -d
```

### Environment variables

See [`.env.example`](.env.example). Key values:

| Variable | Used by |
|----------|---------|
| `RABBITMQ_USER` / `RABBITMQ_PASS` | Admin — management UI and init scripts only |
| `RABBITMQ_APP_USER` / `RABBITMQ_APP_PASS` | Application services (AMQP) |
| `KAFKA_REPLICATION_FACTOR` | Topic creation (`infra/kafka/init-topics.sh`) |

After completing the tutorial, `.env.example` also includes service URLs (`RABBITMQ_URL`, `KAFKA_BROKERS`, mock provider URLs).

### Networking

| Client runs on | RabbitMQ | Kafka bootstrap |
|----------------|----------|-----------------|
| Docker container | `amqp://eda_app:PASS@rabbitmq:5672/eda` | `kafka:9092` |
| Host (your machine) | `amqp://eda_app:PASS@localhost:5672/eda` | `localhost:9094` |

## Useful commands

```bash
# Stop stack
docker compose -f docker-compose.yml -f docker-compose.dev.yml down

# Stop and remove broker data
docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v

# Follow logs
docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f rabbitmq kafka

# List Kafka topics
docker exec eda-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

## Design decisions

- **KRaft Kafka** — no ZooKeeper; current default for new deployments
- **Explicit topology** — queues and topics defined in `infra/`, not auto-created at runtime
- **Separate RabbitMQ users** — admin for ops, `eda_app` for services
- **Monorepo** — infra, services, and shared contracts in one repository (see tutorial)
- **Quorum queues + DLQ** — durable command processing with a dead-letter path for failures

## License

Study / educational use. No license specified.
