# Event-Driven Architecture — Manual Implementation Tutorial

## Introduction

> **Goal:** Build the full payment/order flow using NestJS + Fastify, `@nestjs/microservices`, RabbitMQ (point-to-point), and Kafka (pub/sub) — entirely by hand, one checkbox at a time. Mocks use a pnpm workspace (Phases 0–4); microservices are **standalone Nest projects** in one repo (Phases 5–10).

**Stack:** Node.js 20+, TypeScript, NestJS with Fastify adapter, `@nestjs/microservices`, standalone Nest projects (Phases 5+), pnpm workspaces for mocks/contracts (Phases 0–4), mock Stripe/SendGrid.

**How to use this tutorial:**

1. Work through phases in order — each phase depends on the previous one.
2. Check off each `- [ ]` step as you complete it.
3. Run every command and compare output to the **Expected** block.
4. Stop at each **Checkpoint** and verify before continuing.
5. Commit at the end of each phase using the suggested commit message.

---

## Table of contents

| Phase | Topic | Time |
|-------|-------|------|
| [0](#phase-0--verify-messaging-infrastructure) | Verify messaging infrastructure | ~15 min |
| [1](#phase-1--monorepo-scaffolding) | Monorepo scaffolding | ~20 min |
| [1.5](#phase-15--nest-cli--nestjsmicroservices-foundation) | Nest CLI & `@nestjs/microservices` | ~25 min |
| [2](#phase-2--shared-nestjs-patterns) | Shared NestJS patterns | ~10 min |
| [3](#phase-3--mock-stripe-service) | Mock Stripe service | ~25 min |
| [4](#phase-4--mock-sendgrid-service) | Mock SendGrid service | ~15 min |
| [5](#phase-5--api-gateway--order-service) | API Gateway / Order Service | ~45 min |
| [6](#phase-6--payment-service) | Payment Service | ~40 min |
| [7](#phase-7--availability-service) | Availability Service | ~20 min |
| [8](#phase-8--analytics-service) | Analytics Service | ~20 min |
| [9](#phase-9--invoice-service) | Invoice Service | ~30 min |
| [10](#phase-10--notification-service) | Notification Service | ~25 min |
| [11](#phase-11--wire-everything-in-docker-compose) | Docker Compose wiring | ~30 min |
| [12](#phase-12--end-to-end-manual-test) | End-to-end test | ~15 min |
| [13](#phase-13--production-notes) | Production notes | read-only |

---

## Prerequisites

- **Docker Desktop** running (macOS/Windows/Linux)
- **Node.js 20+** — verify: `node -v` → `v20.x.x` or higher
- **pnpm 9+** — enable via Corepack (ships with Node 20): `corepack enable` — verify: `pnpm -v` → `9.x.x` or higher
- **Nest CLI** — via `npx nest` (uses `@nestjs/cli` from root `devDependencies`, see [Phase 1.5](#phase-15--nest-cli--nestjsmicroservices-foundation)) — verify: `npx nest --version`
- **curl** — for HTTP/SSE testing
- Repository cloned at `event-driven-architecture/`

---

## Architecture

```mermaid
flowchart LR
  Frontend -->|POST_startPayment| ApiGateway
  ApiGateway -->|SSE| Frontend
  ApiGateway -->|orders.payment.requested| RabbitMQ
  RabbitMQ --> PaymentService
  PaymentService --> StripeMock
  StripeMock -->|webhook_success| PaymentService
  PaymentService -->|orders.payment.succeeded| Kafka
  Kafka --> ApiGateway
  Kafka --> AvailabilityService
  Kafka --> AnalyticsService
  Kafka --> InvoiceService
  InvoiceService -->|billing.invoice.created| Kafka
  Kafka --> AnalyticsService
  Kafka --> NotificationService
  NotificationService --> SendGridMock
  SendGridMock -->|Event_Webhook| NotificationService
  NotificationService -->|notifications.email.delivered| RabbitMQ
  NotificationService -->|notifications.email.failed| RabbitMQ
  RabbitMQ --> AnalyticsService
```

### GCP diagram → local stack

| GCP (original diagram) | Your stack | Pattern |
|------------------------|------------|---------|
| Cloud Task | **RabbitMQ** | Point-to-point: one worker per message |
| Pub/Sub | **Kafka** | Fan-out: many consumers per topic |
| Stripe | `mocks/stripe-mock` | HTTP + webhook callback |
| SendGrid | `mocks/sendgrid-mock` | HTTP Mail Send + Event Webhook callback |

### Event payloads

| Event | Transport | Payload |
|-------|-----------|---------|
| `orders.payment.requested` | RabbitMQ routing key | `{ reserveId, orderNumber, amount, customerEmail }` |
| `orders.payment.succeeded` | Kafka topic | `{ reserveId, value, customerInfo, orderNumber }` |
| `orders.payment.failed` | Kafka topic | `{ orderNumber, reason }` |
| `billing.invoice.created` | Kafka topic | `{ amount, customerInfo, orderNumber, invoiceId }` |
| `notifications.email.delivered` | RabbitMQ routing key | `{ invoiceId, orderNumber, email, sgMessageId }` |
| `notifications.email.failed` | RabbitMQ routing key | `{ invoiceId, orderNumber, email, reason }` |

### Naming convention (namespaced)

Commands and events use a **`<domain>.<entity>.<action>`** pattern so multiple teams can share the same brokers without collisions:

| Kind | Pattern | Examples |
|------|---------|----------|
| Command (RabbitMQ) | `<domain>.<entity>.<verb>` | `orders.payment.requested`, `notifications.email.delivered`, `notifications.email.failed` |
| Event (Kafka) | `<domain>.<entity>.<past-tense>` | `orders.payment.succeeded`, `orders.payment.failed`, `billing.invoice.created` |
| Dead letter | `<command>.dlq` / `<command>.failed` | `orders.payment.requested.dlq` |

Define names once in `packages/contracts` (`ROUTING_KEYS`, `TOPICS`, Zod schemas) — microservices install it via `file:../../packages/contracts`; never hardcode strings in services. Validate every payload with the matching `*Schema.parse(...)` at the service boundary.

### Final repo layout

```text
event-driven-architecture/
├── .cursor/tutorial.md
├── biome.json                # root formatter/linter — extended by each microservice
├── package.json              # root tooling (Biome, contracts build, mocks workspace)
├── nest-cli.json             # monorepo registry — mocks only (Phases 3–4)
├── tsconfig.base.json
├── docker-compose.yml
├── docker-compose.dev.yml
├── .env.example
├── packages/
│   ├── contracts/            # shared event schemas — linked into each microservice
│   └── shared/               # used by mocks (Phases 3–4)
├── services/                 # independent Nest apps from Phase 5 onward
│   ├── api-gateway/          # own package.json, nest-cli.json, biome.json, node_modules
│   ├── payment/
│   ├── availability/
│   ├── analytics/
│   ├── invoice/
│   └── notification/
└── mocks/
    ├── stripe-mock/
    └── sendgrid-mock/
```

---

## Phase 0 — Verify messaging infrastructure

**Goal:** Confirm RabbitMQ and Kafka are running with the correct topology. Do **not** recreate infra files — they already exist.

### Step 0.1 — Create `.env`

- [x] Copy the environment template:

```bash
cp .env.example .env
```

- [x] Open `.env` and confirm these values exist (defaults are fine for local dev):

```bash
RABBITMQ_USER=admin
RABBITMQ_PASS=change-me-admin-password
RABBITMQ_APP_USER=eda_app
RABBITMQ_APP_PASS=change-me-app-password
KAFKA_REPLICATION_FACTOR=1
KAFKA_MIN_INSYNC_REPLICAS=1
KAFKA_RETENTION_MS=604800000
```

### Step 0.2 — Start the messaging stack

- [x] Run:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

**Expected:** All long-running services reach `healthy` or `running`. Init containers `eda-rabbitmq-init` and `eda-kafka-init` exit with code `0`.

- [x] Check status:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml ps
```

**Expected:** `eda-rabbitmq`, `eda-kafka`, `eda-kafka-ui` are `Up (healthy)`. Init containers show `Exited (0)`.

### Step 0.3 — Verify RabbitMQ topology

- [x] Open RabbitMQ Management UI: http://localhost:15672
- [x] Login: `admin` / `change-me-admin-password` (or your `.env` values)
- [x] Confirm:
  - Vhost **`eda`** exists
  - Exchange **`eda.commands`** (direct, durable)
  - Queue **`orders.payment.requested`** (quorum, with DLX → `eda.dlx`)
  - Queue **`orders.payment.requested.dlq`**
  - Queue **`notifications.email.delivered`** (quorum, with DLX → `eda.dlx`)
  - Queue **`notifications.email.delivered.dlq`**
  - Queue **`notifications.email.failed`** (quorum, with DLX → `eda.dlx`)
  - Queue **`notifications.email.failed.dlq`**
  - Application user **`eda_app`** exists with permissions on vhost `eda`

Topology is defined in `infra/rabbitmq/definitions.json` and imported by `infra/rabbitmq/init.sh`.

### Step 0.4 — Verify Kafka topics

- [x] Open Kafka UI: http://localhost:8080
- [x] Confirm topics exist:
  - **`orders.payment.succeeded`** — 6 partitions
  - **`billing.invoice.created`** — 3 partitions

Topics are created by `infra/kafka/init-topics.sh`. Auto-create is **disabled** in `docker-compose.yml` (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`).

### Step 0.5 — Verify from CLI (optional)

- [x] List Kafka topics:

```bash
docker exec eda-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Expected:**

```text
billing.invoice.created
orders.payment.succeeded
```

### Checkpoint

- RabbitMQ vhost, queues, and app user are ready.
- Kafka topics exist with correct partition counts.
- You understand: **RabbitMQ = commands (1 consumer)**, **Kafka = events (many consumers)**.

### Suggested commit

No commit needed — infra already exists. Optionally:

```bash
git add .env
# Do NOT commit .env if gitignored — only commit if you intentionally track it
```

---

## Phase 1 — Monorepo scaffolding

**Goal:** Create the pnpm workspace root and the `@eda/contracts` shared package.

### Step 1.1 — Root `package.json`

- [x] Create `package.json` at the repository root:

```json
{
  "name": "event-driven-architecture",
  "private": true,
  "packageManager": "pnpm@9.15.0",
  "scripts": {
    "build": "pnpm --filter @eda/contracts build && pnpm --filter @eda/shared build",
    "build:all": "pnpm run build && pnpm -r --if-present build"
  },
  "devDependencies": {
    "@nestjs/cli": "^10.4.9",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "@types/amqplib": "^0.10.6",
    "@types/node": "^20.17.10",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "typescript": "^5.7.2"
  }
}
```

- [x] Create `pnpm-workspace.yaml` at the repository root:

```yaml
packages:
  - 'packages/*'
  - 'services/*'
  - 'mocks/*'
```

### Step 1.2 — Root TypeScript config

- [x] Create `tsconfig.base.json`:

```json
{
  "compilerOptions": {
    "module": "commonjs",
    "declaration": true,
    "removeComments": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "allowSyntheticDefaultImports": true,
    "target": "ES2021",
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": ".",
    "incremental": true,
    "skipLibCheck": true,
    "strict": true,
    "forceConsistentCasingInFileNames": true,
    "noFallthroughCasesInSwitch": true,
    "esModuleInterop": true
  }
}
```

### Step 1.3 — Contracts package

- [x] Create `packages/contracts/package.json`:

```json
{
  "name": "@eda/contracts",
  "version": "1.0.0",
  "private": true,
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json"
  },
  "dependencies": {
    "zod": "^3.24.1"
  }
}
```

- [x] Create `packages/contracts/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src/**/*"]
}
```

- [x] Create `packages/contracts/src/routing-keys.ts`:

```typescript
export const ROUTING_KEYS = {
  PAYMENT_REQUESTED: 'orders.payment.requested',
} as const;

export const EXCHANGES = {
  COMMANDS: 'eda.commands',
} as const;
```

- [x] Append to `packages/contracts/src/routing-keys.ts` → `ROUTING_KEYS`:

```typescript
EMAIL_DELIVERED: 'notifications.email.delivered',
EMAIL_FAILED: 'notifications.email.failed',
```

- [x] Create `packages/contracts/src/topics.ts`:

```typescript
export const TOPICS = {
  PAYMENT_SUCCEEDED: 'orders.payment.succeeded',
  INVOICE_CREATED: 'billing.invoice.created',
} as const;
```

- [x] Create `packages/contracts/src/events/payment-requested.ts`:

```typescript
import { z } from 'zod';

export const PaymentRequestedSchema = z.object({
  reserveId: z.string().uuid(),
  orderNumber: z.string().uuid(),
  amount: z.number().positive(),
  customerEmail: z.string().email(),
});

export type PaymentRequested = z.infer<typeof PaymentRequestedSchema>;
```

- [x] Create `packages/contracts/src/events/payment-succeeded.ts`:

```typescript
import { z } from 'zod';

export const CustomerInfoSchema = z.object({
  email: z.string().email(),
});

export const PaymentSucceededSchema = z.object({
  reserveId: z.string().uuid(),
  value: z.number().positive(),
  customerInfo: CustomerInfoSchema,
  orderNumber: z.string().uuid(),
});

export type PaymentSucceeded = z.infer<typeof PaymentSucceededSchema>;
```

- [x] Create `packages/contracts/src/events/invoice-created.ts`:

```typescript
import { z } from 'zod';
import { CustomerInfoSchema } from './payment-succeeded';

export const InvoiceCreatedSchema = z.object({
  value: z.number().positive(),
  customerInfo: CustomerInfoSchema,
  orderNumber: z.string().uuid(),
  invoiceId: z.string().uuid(),
});

export type InvoiceCreated = z.infer<typeof InvoiceCreatedSchema>;
```

- [x] Create `packages/contracts/src/events/sendgrid-webhook.ts`:

```typescript
import { z } from 'zod';

export const SendGridWebhookEventSchema = z.object({
  event: z.enum([
    'processed',
    'delivered',
    'bounce',
    'dropped',
    'deferred',
    'open',
    'click',
  ]),
  email: z.string().email(),
  timestamp: z.number(),
  sg_message_id: z.string(),
  invoiceId: z.string().uuid().optional(),
  orderNumber: z.string().uuid().optional(),
  reason: z.string().optional(),
  response: z.string().optional(),
  type: z.string().optional(),
});

export const SendGridWebhookPayloadSchema = z.array(SendGridWebhookEventSchema);

export type SendGridWebhookEvent = z.infer<typeof SendGridWebhookEventSchema>;
```

- [x] Create `packages/contracts/src/events/email-delivered.ts`:

```typescript
import { z } from 'zod';

export const EmailDeliveredSchema = z.object({
  invoiceId: z.string().uuid(),
  orderNumber: z.string().uuid(),
  email: z.string().email(),
  sgMessageId: z.string(),
});

export type EmailDelivered = z.infer<typeof EmailDeliveredSchema>;
```

- [x] Create `packages/contracts/src/events/email-failed.ts`:

```typescript
import { z } from 'zod';

export const EmailFailedSchema = z.object({
  invoiceId: z.string().uuid(),
  orderNumber: z.string().uuid(),
  email: z.string().email(),
  reason: z.string(),
});

export type EmailFailed = z.infer<typeof EmailFailedSchema>;
```

- [x] Update `packages/contracts/src/index.ts`:

```typescript
export * from './routing-keys';
export * from './topics';
export * from './events/payment-requested';
export * from './events/payment-succeeded';
export * from './events/invoice-created';
export * from './events/sendgrid-webhook';
export * from './events/email-delivered';
export * from './events/email-failed';
```

- [x] Append to `infra/rabbitmq/definitions.json` → `queues`:

```json
{
  "name": "notifications.email.delivered",
  "vhost": "eda",
  "durable": true,
  "auto_delete": false,
  "arguments": {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": "eda.dlx",
    "x-dead-letter-routing-key": "notifications.email.delivered.failed"
  }
},
{
  "name": "notifications.email.delivered.dlq",
  "vhost": "eda",
  "durable": true,
  "auto_delete": false,
  "arguments": {
    "x-queue-type": "quorum"
  }
},
{
  "name": "notifications.email.failed",
  "vhost": "eda",
  "durable": true,
  "auto_delete": false,
  "arguments": {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": "eda.dlx",
    "x-dead-letter-routing-key": "notifications.email.failed.failed"
  }
},
{
  "name": "notifications.email.failed.dlq",
  "vhost": "eda",
  "durable": true,
  "auto_delete": false,
  "arguments": {
    "x-queue-type": "quorum"
  }
}
```

- [x] Append to `infra/rabbitmq/definitions.json` → `bindings`:

```json
{
  "source": "eda.commands",
  "vhost": "eda",
  "destination": "notifications.email.delivered",
  "destination_type": "queue",
  "routing_key": "notifications.email.delivered",
  "arguments": {}
},
{
  "source": "eda.dlx",
  "vhost": "eda",
  "destination": "notifications.email.delivered.dlq",
  "destination_type": "queue",
  "routing_key": "notifications.email.delivered.failed",
  "arguments": {}
},
{
  "source": "eda.commands",
  "vhost": "eda",
  "destination": "notifications.email.failed",
  "destination_type": "queue",
  "routing_key": "notifications.email.failed",
  "arguments": {}
},
{
  "source": "eda.dlx",
  "vhost": "eda",
  "destination": "notifications.email.failed.dlq",
  "destination_type": "queue",
  "routing_key": "notifications.email.failed.failed",
  "arguments": {}
}
```

- [x] Recreate RabbitMQ topology:

```bash
docker compose up -d rabbitmq-init
```

### Step 1.4 — Install and build

- [x] From the repository root:

```bash
pnpm install
pnpm --filter @eda/contracts build
```

**Expected:** `packages/contracts/dist/` is created with compiled `.js` and `.d.ts` files. No TypeScript errors.

### Checkpoint

- `pnpm --filter @eda/contracts build` succeeds.
- You can import `@eda/contracts` from other workspace packages.

### Suggested commit

```bash
git add package.json pnpm-workspace.yaml tsconfig.base.json packages/contracts pnpm-lock.yaml
git commit -m "feat: add monorepo scaffolding and shared event contracts"
```

---

## Phase 1.5 — Nest CLI & `@nestjs/microservices` foundation

**Goal:** Install microservice transport dependencies, create an empty Nest monorepo skeleton for **mocks** (Phases 3–4), and learn the `@nestjs/microservices` patterns used throughout the tutorial. **Microservices (Phases 5–10) use standalone `npx nest new` projects** — see [Independent services](#independent-services-from-phase-5-onward).

### Step 1.5.1 — Verify Nest CLI (`npx nest`)

The CLI is already listed in root `devDependencies` (Phase 1). Use `npx nest` — no global install needed.

- [x] From the repository root:

```bash
pnpm install
npx nest --version
```

**Expected:** Prints `@nestjs/cli` version (10.x), resolved from local `node_modules`.

### Step 1.5.2 — Add microservice dependencies to root

- [x] Update root `package.json` — add these to `devDependencies` (shared across all Nest apps via pnpm hoisting):

```json
"@nestjs/microservices": "^10.4.15",
"amqplib": "^0.10.5",
"amqp-connection-manager": "^4.0.0",
"kafkajs": "^2.2.4"
```

- [x] Run:

```bash
pnpm install
```

**Expected:** `node_modules` contains `@nestjs/microservices`, `amqplib`, and `kafkajs`.

### Step 1.5.3 — Root `nest-cli.json` (empty monorepo skeleton)

Per [Nest CLI workspaces](https://docs.nestjs.com/cli/monorepo), one root `nest-cli.json` holds the monorepo registry. Start with an **empty** `projects` object — you will add each app when you implement it.

- [x] Create `nest-cli.json` at the repository root:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "monorepo": true,
  "root": ".",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  },
  "projects": {}
}
```

> **Do not** pre-register `api-gateway`, `payment`, etc. here. Each phase adds its own entry under `projects`.

### Step 1.5.4 — How to register a mock project (Phases 3–4)

When you start a **mock** app, add an entry to root `nest-cli.json` → `projects`:

```json
"<project-name>": {
  "type": "application",
  "root": "<path/to/app>",
  "entryFile": "main",
  "sourceRoot": "<path/to/app>/src",
  "compilerOptions": {
    "tsConfigPath": "<path/to/app>/tsconfig.json"
  }
}
```

**Example** — Phase 3 (`stripe-mock`):

```json
"stripe-mock": {
  "type": "application",
  "root": "mocks/stripe-mock",
  "entryFile": "main",
  "sourceRoot": "mocks/stripe-mock/src",
  "compilerOptions": {
    "tsConfigPath": "mocks/stripe-mock/tsconfig.json"
  }
}
```

If it is the **first** project registered in the monorepo, also update the top-level defaults:

```json
"root": "mocks/stripe-mock",
"sourceRoot": "mocks/stripe-mock/src"
```

Individual per-app `nest-cli.json` files are **not** needed for mocks (Phases 3–4).

> **Phases 5–10** use **standalone Nest projects** under `services/` — each with its own `nest-cli.json`. See [Independent services (from Phase 5 onward)](#independent-services-from-phase-5-onward).

### Step 1.5.5 — Microservice patterns used in this project

| Pattern | When to use | Nest API |
|---------|-------------|----------|
| **Hybrid app** | HTTP + messaging (api-gateway, payment, analytics…) | `connectMicroservice()` + `startAllMicroservices()` + `listen()` |
| **Fire-and-forget command/event** | RabbitMQ commands, Kafka events | `@EventPattern()` + `client.emit()` |
| **Request/response** | Not used in this EDA flow | `@MessagePattern()` + `client.send()` |
| **RabbitMQ manual ack** | Payment consumer (DLQ on failure) | `noAck: false` + `RmqContext.getChannelRef().ack/nack()` |
| **Kafka consumer group** | One group per service | `consumer: { groupId: '…' }` in transport options |

**RabbitMQ transport options** (match your `infra/rabbitmq/definitions.json`):

```typescript
{
  transport: Transport.RMQ,
  options: {
    urls: [process.env.RABBITMQ_URL!],
    queue: 'orders.payment.requested',
    noAck: false,
    prefetchCount: 1,
    queueOptions: { durable: true },
    wildcards: true,
    exchange: 'eda.commands',
    exchangeType: 'topic',
  },
}
```

**Kafka transport options:**

```typescript
{
  transport: Transport.KAFKA,
  options: {
    client: { clientId: 'my-service', brokers: process.env.KAFKA_BROKERS!.split(',') },
    consumer: { groupId: 'my-service' },
    subscribe: { fromBeginning: false },
  },
}
```

### Step 1.5.6 — Nest CLI cheat sheet

**Mocks (Phases 3–4)** — from the **repository root**:

```bash
npx nest build stripe-mock
npx nest start stripe-mock --watch
npx nest g module stripe --project stripe-mock --no-spec
```

**Microservices (Phases 5–10)** — from **inside** `services/<name>/`:

```bash
cd services/api-gateway
pnpm start:dev          # nest start --watch (local nest-cli.json)
pnpm build              # nest build
npx nest g module orders --no-spec
npx nest g controller orders --no-spec
```

### Step 1.5.7 — Verify CLI works

- [x] Run:

```bash
npx nest --version
```

**Expected:** Prints CLI version. `nest-cli.json` exists with `"projects": {}`.

### Checkpoint

- `nest-cli.json` exists at repo root with an **empty** `projects` object.
- `@nestjs/microservices`, `amqplib`, `amqp-connection-manager`, and `kafkajs` are installed.
- You know how to register a project incrementally (Step 1.5.4) and use `npx nest`.
- You understand hybrid apps, `@EventPattern`, and `client.emit()`.

### Suggested commit

```bash
git add nest-cli.json package.json pnpm-lock.yaml
git commit -m "feat: add Nest monorepo skeleton and microservices dependencies"
```

---

## Phase 2 — Shared NestJS patterns

**Goal:** Create `@eda/shared` with utilities every service reuses: health check, idempotency store, env helper.

### Step 2.1 — Shared package scaffold

- [x] Create `packages/shared/package.json`:

```json
{
  "name": "@eda/shared",
  "version": "1.0.0",
  "private": true,
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@nestjs/common": "^10.4.15"
  }
}
```

- [x] Create `packages/shared/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src/**/*"]
}
```

### Step 2.2 — Idempotency store

Every consumer in the diagram is marked **Idempotent**. For this tutorial, use an in-memory store. In production, replace with Redis or a database table.

- [x] Create `packages/shared/src/idempotency.store.ts`:

```typescript
import { Injectable } from '@nestjs/common';

@Injectable()
export class IdempotencyStore {
  private readonly processed = new Set<string>();

  /** Returns true if this id was already processed (duplicate). */
  isDuplicate(id: string): boolean {
    return this.processed.has(id);
  }

  markProcessed(id: string): void {
    this.processed.add(id);
  }
}
```

### Step 2.3 — Env helper

- [x] Create `packages/shared/src/env.ts`:

```typescript
export function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}
```

### Step 2.4 — Health controller

- [x] Create `packages/shared/src/health.controller.ts`:

```typescript
import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class HealthController {
  @Get()
  check() {
    return { status: 'ok' };
  }
}
```

### Step 2.5 — Barrel export

- [x] Create `packages/shared/src/index.ts`:

```typescript
export * from './idempotency.store';
export * from './env';
export * from './health.controller';
```

### Step 2.6 — Build shared package

- [x] Run:

```bash
pnpm install
pnpm --filter @eda/shared build
```

**Expected:** `packages/shared/dist/` created successfully.

### Conventions used in every service

| Convention | Mocks (Phases 3–4) | Microservices (Phases 5–10) |
|------------|--------------------|-----------------------------|
| Scaffold | Register in root `nest-cli.json` | `npx nest new <name>` in `services/` |
| Dev server | `npx nest start <project> --watch` from repo root | `pnpm start:dev` from inside the service folder |
| Build | `npx nest build <project>` from repo root | `pnpm build` from inside the service folder |
| HTTP adapter | `NestFactory.create(AppModule, new FastifyAdapter())` | same |
| Hybrid apps | `connectMicroservice()` before `startAllMicroservices()` + `listen()` | same |
| Commands (RabbitMQ) | `@EventPattern` in `*.handler.ts`; publish via `*CommandPublisher` port | same |
| Events (Kafka) | `@EventPattern` in `*.handler.ts`; publish via `*EventPublisher` port | same |
| External HTTP | Gateway interface in `gateways/` | `PaymentGateway`, `EmailGateway` |
| SSE | `OrderStatusStreamService` — separate from repository | api-gateway only |
| Transport package | `@nestjs/microservices` (`Transport.RMQ`, `Transport.KAFKA`) | same |
| Health | `GET /health` via `HealthController` | same (`src/common/health.controller.ts`) |
| Env vars | `requireEnv('VAR_NAME')` at startup | same (`src/common/env.ts` + `dotenv`) |
| Idempotency key | `orderNumber` for payment flow; `invoiceId` for invoice flow | same |
| Kafka message key | Always `orderNumber` in production; Nest `emit()` sends value only (partition routing is a production refinement) | same |
| Consumer groups | One group per service: `api-gateway`, `payment-service`, etc. | same |

### Checkpoint

- `@eda/shared` builds and exports health, env, and idempotency utilities.

### Suggested commit

```bash
git add packages/shared
git commit -m "feat: add shared NestJS utilities for health and idempotency"
```

---

## Phase 3 — Mock Stripe service

**Goal:** Simulate Stripe PaymentIntent creation and webhook delivery to the Payment Service.

**Port:** `3001` | **Package:** `@eda/stripe-mock` | **Nest project:** `stripe-mock`

### Step 3.1 — Register project and scaffold

- [x] Register `stripe-mock` in root `nest-cli.json` → `projects` (see [Step 1.5.4](#step-154--how-to-register-a-mock-project-phases-34)):

```json
"stripe-mock": {
  "type": "application",
  "root": "mocks/stripe-mock",
  "entryFile": "main",
  "sourceRoot": "mocks/stripe-mock/src",
  "compilerOptions": {
    "tsConfigPath": "mocks/stripe-mock/tsconfig.json"
  }
}
```

- [x] Set top-level defaults (first project in the monorepo):

```json
"root": "mocks/stripe-mock",
"sourceRoot": "mocks/stripe-mock/src"
```

- [x] Create `mocks/stripe-mock/package.json`:

```json
{
  "name": "@eda/stripe-mock",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build stripe-mock --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start stripe-mock --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [x] Create `mocks/stripe-mock/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src/**/*"]
}
```

### Step 3.2 — Stripe mock implementation

- [x] Create `mocks/stripe-mock/src/stripe.controller.ts`:

```typescript
import { Body, Controller, Post } from '@nestjs/common';
import { StripeService } from './stripe.service';

@Controller('v1')
export class StripeController {
  constructor(private readonly stripeService: StripeService) {}

  @Post('payment-intents')
  createPaymentIntent(
    @Body() body: { orderNumber: string; amount: number; reserveId: string; customerEmail: string },
  ) {
    return this.stripeService.createPaymentIntent(body);
  }
}
```

- [x] Create `mocks/stripe-mock/src/stripe.service.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { requireEnv } from '@eda/shared';
import { randomUUID } from 'crypto';

@Injectable()
export class StripeService {
  private readonly logger = new Logger(StripeService.name);
  private readonly webhookUrl = requireEnv('PAYMENT_WEBHOOK_URL');

  createPaymentIntent(body: {
    orderNumber: string;
    amount: number;
    reserveId: string;
    customerEmail: string;
  }) {
    const intentId = `pi_mock_${randomUUID()}`;
    this.logger.log(
      `Created PaymentIntent ${intentId} for order ${body.orderNumber}`,
    );

    // Simulate async Stripe webhook delivery
    setTimeout(() => {
      void this.sendWebhook(body);
    }, 500);

    return { id: intentId, status: 'processing' };
  }

  private async sendWebhook(body: {
    orderNumber: string;
    amount: number;
    reserveId: string;
    customerEmail: string;
  }) {
    const payload = {
      type: 'payment_intent.succeeded',
      data: {
        orderNumber: body.orderNumber,
        amount: body.amount,
        reserveId: body.reserveId,
        customerEmail: body.customerEmail,
      },
    };

    this.logger.log(`Sending webhook to ${this.webhookUrl}`);

    const response = await fetch(this.webhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      this.logger.error(`Webhook failed: HTTP ${response.status}`);
    }
  }
}
```

- [x] Create `mocks/stripe-mock/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { StripeController } from './stripe.controller';
import { StripeService } from './stripe.service';

@Module({
  controllers: [HealthController, StripeController],
  providers: [StripeService],
})
export class AppModule {}
```

- [x] Create `mocks/stripe-mock/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { AppModule } from './app.module';
import { requireEnv } from '@eda/shared';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3001);
  await app.listen(port, '0.0.0.0');
  console.log(`stripe-mock listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 3.3 — Build and run locally (optional smoke test)

- [x] Run:

```bash
pnpm install
pnpm --filter @eda/stripe-mock build
PAYMENT_WEBHOOK_URL=http://localhost:3010/webhooks/stripe PORT=3001 pnpm --filter @eda/stripe-mock start
```

**Expected:** `stripe-mock listening on 3001` (Payment Service is not running yet — webhook will fail; that is OK for now).

Press `Ctrl+C` to stop.

### Checkpoint

- `@eda/stripe-mock` builds.
- `POST /v1/payment-intents` returns a mock intent ID.

### Suggested commit

```bash
git add nest-cli.json mocks/stripe-mock
git commit -m "feat: add mock Stripe service with webhook simulation"
```

---

## Phase 4 — Mock SendGrid service

**Goal:** Simulate SendGrid Mail Send API (202 Accepted) and async Event Webhook delivery.

**Port:** `3002` | **Package:** `@eda/sendgrid-mock` | **Nest project:** `sendgrid-mock`

### Step 4.1 — Register project and scaffold

- [x] Register `sendgrid-mock` in root `nest-cli.json` → `projects`:

```json
"sendgrid-mock": {
  "type": "application",
  "root": "mocks/sendgrid-mock",
  "entryFile": "main",
  "sourceRoot": "mocks/sendgrid-mock/src",
  "compilerOptions": {
    "tsConfigPath": "mocks/sendgrid-mock/tsconfig.json"
  }
}
```

- [x] Create `mocks/sendgrid-mock/package.json`:

```json
{
  "name": "@eda/sendgrid-mock",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build sendgrid-mock --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start sendgrid-mock --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [x] Create `mocks/sendgrid-mock/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "dist",
    "rootDir": "src"
  },
  "include": ["src/**/*"]
}
```

### Step 4.2 — SendGrid mock implementation

- [x] Create `mocks/sendgrid-mock/.env`:

```bash
PORT=3002
NOTIFICATION_WEBHOOK_URL=http://localhost:3050/webhooks/sendgrid
```

- [x] Create `mocks/sendgrid-mock/src/mail.types.ts`:

```typescript
export interface SendMailRequestDto {
  personalizations: Array<{
    to: Array<{ email: string }>;
    custom_args?: Record<string, string>;
  }>;
  subject: string;
}
```

- [x] Replace `mocks/sendgrid-mock/src/mail.controller.ts`:

```typescript
import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { MailService } from './mail.service';
import { SendMailRequestDto } from './mail.types';

@Controller('v3/mail')
export class MailController {
  constructor(private readonly mailService: MailService) {}

  @Post('send')
  @HttpCode(202)
  send(@Body() body: SendMailRequestDto) {
    return this.mailService.sendMail(body);
  }
}
```

- [x] Replace `mocks/sendgrid-mock/src/mail.service.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { requireEnv } from '@eda/shared';
import { Injectable, Logger } from '@nestjs/common';
import { SendMailRequestDto } from './mail.types';

@Injectable()
export class MailService {
  private readonly logger = new Logger(MailService.name);
  private readonly webhookUrl = requireEnv('NOTIFICATION_WEBHOOK_URL');

  sendMail(body: SendMailRequestDto): { message: string } {
    const personalization = body.personalizations?.[0];
    const email = personalization?.to?.[0]?.email ?? 'unknown';
    const customArgs = personalization?.custom_args ?? {};
    const sgMessageId = `sg_mock_${randomUUID()}`;

    this.logger.log(`Accepted email to ${email}: ${body.subject}`);

    setTimeout(() => {
      void this.emitEvents(email, sgMessageId, customArgs);
    }, 2000);

    return { message: 'accepted' };
  }

  private async emitEvents(
    email: string,
    sgMessageId: string,
    customArgs: Record<string, string>,
  ): Promise<void> {
    const shouldBounce = email.includes('bounce@');

    const base = {
      email,
      timestamp: Math.floor(Date.now() / 1000),
      sg_message_id: sgMessageId,
      ...customArgs,
    };

    await this.postWebhook([{ ...base, event: 'processed' }]);

    if (shouldBounce) {
      await this.postWebhook([
        {
          ...base,
          event: 'bounce',
          reason: '500 unknown recipient',
          type: 'bounce',
        },
      ]);
      return;
    }

    await this.postWebhook([
      { ...base, event: 'delivered', response: '250 OK' },
    ]);
  }

  private async postWebhook(events: unknown[]): Promise<void> {
    const response = await fetch(this.webhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(events),
    });

    if (!response.ok) {
      this.logger.error(`SendGrid webhook failed: HTTP ${response.status}`);
    }
  }
}
```

- [x] Create `mocks/sendgrid-mock/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { MailController } from './mail.controller';
import { MailService } from './mail.service';

@Module({
  controllers: [HealthController, MailController],
  providers: [MailService],
})
export class AppModule {}
```

- [x] Create `mocks/sendgrid-mock/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3002);
  await app.listen(port, '0.0.0.0');
  console.log(`sendgrid-mock listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 4.3 — Build

- [ ] Run:

```bash
pnpm --filter @eda/sendgrid-mock build
```

**Expected:** Build succeeds with no errors.

### Checkpoint

- `@eda/sendgrid-mock` builds.
- Service accepts `POST /v3/mail/send` and returns HTTP 202.
- After ~2s, mock posts Event Webhook array to `NOTIFICATION_WEBHOOK_URL`.
- Email containing `bounce@` triggers `bounce` instead of `delivered`.

### Suggested commit

```bash
git add nest-cli.json mocks/sendgrid-mock infra/rabbitmq/definitions.json packages/contracts
git commit -m "feat: add SendGrid mock with Event Webhook simulation"
```

---

## Independent services (from Phase 5 onward)

Starting here, each microservice is a **standalone NestJS project** — its own `package.json`, `nest-cli.json`, and `node_modules`. One Git repository; six independent deployable apps under `services/`.

| Topic | Phases 0–4 (mocks) | Phases 5–10 (microservices) |
|-------|---------------------|-----------------------------|
| Scaffold | Register in root `nest-cli.json` | `npx nest new <name>` inside `services/` |
| Run dev | `npx nest start <project> --watch` from repo root | `pnpm start:dev` from inside the service folder |
| Build | `npx nest build <project>` from repo root | `pnpm build` from inside the service folder |
| Lint / format | Root `biome.json` (`pnpm lint` from repo root) | Local `biome.json` extending root (`pnpm lint` from service folder) |
| Shared utilities | `@eda/shared` workspace package | Local `src/common/` (copied per service) |
| Event contracts | `@eda/contracts` workspace | `@eda/contracts` via `file:../../packages/contracts` |

> **Why the split?** Mocks (Phases 3–4) stay on the workspace pattern you already built. Microservices match how most teams run day-to-day: open one folder, run one app, ship one Docker image.

### Contracts dependency (every microservice)

From the **repository root**, build contracts once (re-run after changing `packages/contracts/`):

```bash
pnpm --filter @eda/contracts build
```

Then inside each service:

```bash
pnpm add @eda/contracts@file:../../packages/contracts
```

### Local `src/common/` (every microservice)

Phases 3–4 used `@eda/shared`. Standalone services keep the same helpers locally under `src/common/`:

| File | Purpose |
|------|---------|
| `src/common/env.ts` | `requireEnv()` |
| `src/common/health.controller.ts` | `GET /health` |
| `src/common/idempotency.store.ts` | In-memory dedup |
| `src/common/zod-validation.filter.ts` | Map `ZodError` → HTTP 400 |

Phase 5 creates these files. Phases 6–10 copy them: `cp -r ../api-gateway/src/common ./src/common`.

### Biome instead of ESLint/Prettier (every microservice)

From **inside** `services/<name>/`:

```bash
# 1. Remove Nest's default linter/formatter
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier

# 2. Install Biome (same major version as repo root)
pnpm add -D @biomejs/biome@2.5.0 --save-exact

# 3. Create biome.json — nested config extending the repo root (Biome 2 monorepo)
cat > biome.json <<'EOF'
{
  "$schema": "https://biomejs.dev/schemas/2.5.0/schema.json",
  "root": false,
  "extends": ["../../biome.json"]
}
EOF
```

Replace the `format` and `lint` scripts in the service's `package.json`:

```json
"format": "biome format --write .",
"lint": "biome check .",
"lint:fix": "biome check --write ."
```

Remove the old Prettier/ESLint script lines (`"format": "prettier ..."`, `"lint": "eslint ..."`).

Phases 6–10 copy `biome.json` from `api-gateway` but still need the same `package.json` script replacements.

**Why extend root?** Root `biome.json` already has NestJS-friendly settings (`unsafeParameterDecoratorsEnabled`, `useImportType: off`, single quotes, semicolons as needed). Each service keeps its own Biome install and scripts — like a real standalone repo — without duplicating rules.

**Format on save:** Root `.vscode/settings.json` already points at `${workspaceFolder}/node_modules/@biomejs/biome/bin/biome`. Open the repository at its root in VS Code/Cursor; no per-service editor config needed.

**Verify** (from the service folder):

```bash
pnpm lint
pnpm lint:fix
```

**Expected:** No ESLint/Prettier files remain; `pnpm lint` runs Biome on `src/` and `test/`.

### Service layout (Phases 5–10)

| Layer | Folder / file | Role |
|-------|---------------|------|
| Validation | `@eda/contracts` schemas + local `*.schema.ts` | Zod at every inbound/outbound boundary |
| HTTP inbound | `*.controller.ts` | Thin — `Schema.parse(body)`, delegate to service |
| SSE outbound | `*-events.controller.ts` + `*-stream.service.ts` | Push to browser — not in repository |
| Messaging inbound | `*.handler.ts` | Thin `@EventPattern` — `Schema.parse(payload)`, idempotency, delegate |
| Application | `*.service.ts` | Business logic / orchestration — receives typed input only |
| Persistence | `*.repository.ts` + `in-memory-*.repository.ts` | Save / find / update — no messaging, no SSE |
| Command publish | `messaging/*-command.publisher.ts` | `Schema.parse(payload)` then RabbitMQ `emit()` |
| Event publish | `messaging/*-event.publisher.ts` | `Schema.parse(event)` then Kafka `emit()` |
| External HTTP | `gateways/*.gateway.ts` | Stripe, SendGrid — behind interface |

**Zod rules (every microservice):**

- HTTP body / route params → `SomeSchema.parse(...)` in the controller.
- Kafka / RabbitMQ payload → `SomeSchema.parse(payload)` in the handler (`payload: unknown`).
- Before publishing → `SomeSchema.parse(...)` in the publisher implementation.
- Webhooks → schema in `@eda/contracts`, parse in the controller.
- HTTP-only DTOs (e.g. `CreateOrderSchema`) → local `*.schema.ts` in the service; shared events/commands → `@eda/contracts`.
- Register `ZodValidationExceptionFilter` in `main.ts` so HTTP parse errors return `400`.

---

## Phase 5 — API Gateway / Order Service

**Goal:** HTTP entry point — create order, publish `orders.payment.requested`, SSE push on `payment_succeeded` or `payment_failed`. Hybrid app (HTTP + Kafka consumer).

**Port:** `3000` | **Folder:** `services/api-gateway/`

```text
services/api-gateway/src/
├── orders/
│   order.entity.ts
│   create-order.schema.ts
│   orders.repository.ts
│   in-memory-orders.repository.ts
│   orders.service.ts
│   orders.controller.ts
│   orders-events.controller.ts
│   order-status-stream.service.ts
├── messaging/
│   payment-command.publisher.ts
│   rabbitmq-payment-command.publisher.ts
├── payment-events/
│   payment-events.handler.ts
└── common/
    zod-validation.filter.ts
```

### Step 5.1 — Scaffold standalone project

- [x] Create the service with Nest CLI (from **repository root**):

```bash
mkdir -p services
cd services
npx nest new api-gateway --package-manager pnpm --strict --skip-git
```

**Expected:** `services/api-gateway/` with its own `package.json`, `nest-cli.json`, `tsconfig.json`, and `src/` — plus Nest's default `.eslintrc.js` and `.prettierrc` (removed in the next step).

- [x] Swap ESLint/Prettier for Biome (from **`services/api-gateway/`**) — full rationale in [Biome instead of ESLint/Prettier](#biome-instead-of-eslintprettier-every-microservice):

```bash
cd api-gateway
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
```

Create `services/api-gateway/biome.json`:

```json
{
  "$schema": "https://biomejs.dev/schemas/2.5.0/schema.json",
  "root": false,
  "extends": ["../../biome.json"]
}
```

In `services/api-gateway/package.json`, replace the `format` and `lint` scripts:

```json
"format": "biome format --write .",
"lint": "biome check .",
"lint:fix": "biome check --write ."
```

- [x] Install microservice dependencies (still from **`services/api-gateway/`**):

```bash
cd api-gateway
pnpm add @nestjs/platform-fastify @nestjs/microservices amqplib amqp-connection-manager kafkajs dotenv zod
pnpm add -D @types/amqplib
```

- [x] Link event contracts (build first from repo root if needed):

```bash
# from repository root
pnpm --filter @eda/contracts build

# back in services/api-gateway/
pnpm add @eda/contracts@file:../../packages/contracts
```

- [x] Create `services/api-gateway/.env` for local dev (brokers on host):

```bash
PORT=3000
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
KAFKA_BROKERS=localhost:9094
```

- [x] Create local shared helpers — `services/api-gateway/src/common/env.ts`:

```typescript
export function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}
```

- [x] Create `services/api-gateway/src/common/health.controller.ts`:

```typescript
import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class HealthController {
  @Get()
  check() {
    return { status: 'ok' };
  }
}
```

- [x] Create `services/api-gateway/src/common/idempotency.store.ts`:

```typescript
import { Injectable } from '@nestjs/common';

@Injectable()
export class IdempotencyStore {
  private readonly processed = new Set<string>();

  isDuplicate(id: string): boolean {
    return this.processed.has(id);
  }

  markProcessed(id: string): void {
    this.processed.add(id);
  }
}
```

- [x] Create `services/api-gateway/src/common/zod-validation.filter.ts`:

```typescript
import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpStatus,
} from '@nestjs/common';
import { FastifyReply } from 'fastify';
import { ZodError } from 'zod';

@Catch(ZodError)
export class ZodValidationExceptionFilter implements ExceptionFilter {
  catch(error: ZodError, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse<FastifyReply>();

    response.status(HttpStatus.BAD_REQUEST).send({
      statusCode: HttpStatus.BAD_REQUEST,
      error: 'Bad Request',
      message: 'Validation failed',
      issues: error.issues.map((issue) => ({
        path: issue.path.join('.'),
        message: issue.message,
      })),
    });
  }
}
```

- [x] Scaffold feature modules (from **`services/api-gateway/`** — no `--project` flag):

```bash
npx nest g module orders --no-spec
npx nest g service orders --no-spec
npx nest g controller orders --no-spec
npx nest g controller orders-events --no-spec
npx nest g service order-status-stream --no-spec
npx nest g controller payment-events --no-spec
mkdir -p src/messaging
```

**Expected:** Folders `orders/`, `payment-events/`, `messaging/` under `src/`.

> **Note:** This service has its **own** `nest-cli.json`. All `npx nest` commands in Phases 5–10 run from **inside** the service folder unless stated otherwise.

### Step 5.2 — Extend contracts (`orders.payment.failed`)

- [x] Create `packages/contracts/src/events/payment-failed.ts`:

```typescript
import { z } from 'zod';

export const PaymentFailedSchema = z.object({
  orderNumber: z.string().uuid(),
  reason: z.string().min(1),
});

export type PaymentFailed = z.infer<typeof PaymentFailedSchema>;
```

- [x] Add to `packages/contracts/src/topics.ts`:

```typescript
export const TOPICS = {
  PAYMENT_SUCCEEDED: 'orders.payment.succeeded',
  PAYMENT_FAILED: 'orders.payment.failed',
  INVOICE_CREATED: 'billing.invoice.created',
} as const;
```

- [x] Export from `packages/contracts/src/index.ts`:

```typescript
export * from './events/payment-failed';
```

- [x] Append to `infra/kafka/init-topics.sh` (before the final `echo`):

```bash
create_topic "orders.payment.failed" 3
```

- [x] Rebuild contracts and recreate Kafka topics:

```bash
pnpm --filter @eda/contracts build
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d kafka-init
```

**Expected:** `orders.payment.failed` appears in Kafka topic list.

### Step 5.3 — Order domain model

- [x] Create `services/api-gateway/src/orders/order.entity.ts`:

```typescript
export type OrderStatus =
  | 'payment_pending'
  | 'payment_succeeded'
  | 'payment_failed';

export interface Order {
  orderNumber: string;
  reserveId: string;
  productId: string;
  amount: number;
  customerEmail: string;
  status: OrderStatus;
}
```

### Step 5.3.1 — Create order schema (HTTP)

- [x] Create `services/api-gateway/src/orders/create-order.schema.ts`:

```typescript
import { z } from 'zod';

export const CreateOrderSchema = z.object({
  productId: z.string().min(1),
  customerEmail: z.string().email(),
  amount: z.number().positive(),
});

export type CreateOrderInput = z.infer<typeof CreateOrderSchema>;

export const OrderNumberParamSchema = z.string().uuid();
```

### Step 5.4 — Orders repository

- [x] Create `services/api-gateway/src/orders/orders.repository.ts`:

```typescript
import { Order, OrderStatus } from './order.entity';

export const ORDERS_REPOSITORY = Symbol('ORDERS_REPOSITORY');

export interface OrdersRepository {
  save(order: Order): void;
  findByOrderNumber(orderNumber: string): Order | null;
  updateStatus(orderNumber: string, status: OrderStatus): Order | null;
}
```

- [x] Create `services/api-gateway/src/orders/in-memory-orders.repository.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { Order, OrderStatus } from './order.entity';
import { OrdersRepository } from './orders.repository';

@Injectable()
export class InMemoryOrdersRepository implements OrdersRepository {
  private readonly orders = new Map<string, Order>();

  save(order: Order): void {
    this.orders.set(order.orderNumber, order);
  }

  findByOrderNumber(orderNumber: string): Order | null {
    return this.orders.get(orderNumber) ?? null;
  }

  updateStatus(orderNumber: string, status: OrderStatus): Order | null {
    const order = this.orders.get(orderNumber);
    if (!order) return null;
    order.status = status;
    return order;
  }
}
```

### Step 5.5 — Order status stream (SSE)

- [x] Replace `services/api-gateway/src/orders/order-status-stream.service.ts`:

```typescript
import { Injectable, NotFoundException } from '@nestjs/common';
import { map, Observable, Subject } from 'rxjs';
import { OrderStatus } from './order.entity';

export interface OrderStatusEvent {
  orderNumber: string;
  status: OrderStatus;
}

@Injectable()
export class OrderStatusStreamService {
  private readonly streams = new Map<string, Subject<OrderStatusEvent>>();

  register(orderNumber: string): void {
    this.streams.set(orderNumber, new Subject<OrderStatusEvent>());
  }

  watch(orderNumber: string): Observable<{ data: OrderStatusEvent }> {
    const stream = this.streams.get(orderNumber);
    if (!stream) {
      throw new NotFoundException(`Order ${orderNumber} not found`);
    }
    return stream.pipe(map((event) => ({ data: event })));
  }

  push(orderNumber: string, status: OrderStatus): void {
    const stream = this.streams.get(orderNumber);
    if (!stream) return;
    stream.next({ orderNumber, status });
    if (status !== 'payment_pending') {
      stream.complete();
    }
  }
}
```

### Step 5.6 — Payment command publisher

- [x] Create `services/api-gateway/src/messaging/payment-command.publisher.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts';

export const PAYMENT_COMMAND_PUBLISHER = Symbol('PAYMENT_COMMAND_PUBLISHER');

export interface PaymentCommandPublisher {
  publishPaymentRequested(payload: PaymentRequested): Promise<void>;
}
```

- [x] Create `services/api-gateway/src/messaging/rabbitmq-payment-command.publisher.ts`:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import {
  PaymentRequested,
  PaymentRequestedSchema,
  ROUTING_KEYS,
} from '@eda/contracts';
import { firstValueFrom } from 'rxjs';
import { PaymentCommandPublisher } from './payment-command.publisher';

@Injectable()
export class RabbitMqPaymentCommandPublisher implements PaymentCommandPublisher {
  constructor(
    @Inject('RABBITMQ_COMMANDS') private readonly client: ClientProxy,
  ) {}

  async publishPaymentRequested(payload: PaymentRequested): Promise<void> {
    const command = PaymentRequestedSchema.parse(payload);
    await firstValueFrom(
      this.client.emit(ROUTING_KEYS.PAYMENT_REQUESTED, command),
    );
  }
}
```

### Step 5.7 — Orders service

- [x] Replace `services/api-gateway/src/orders/orders.service.ts`:

```typescript
import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  PAYMENT_COMMAND_PUBLISHER,
  PaymentCommandPublisher,
} from '../messaging/payment-command.publisher';
import { CreateOrderInput } from './create-order.schema';
import { Order, OrderStatus } from './order.entity';
import { OrderStatusStreamService } from './order-status-stream.service';
import {
  ORDERS_REPOSITORY,
  OrdersRepository,
} from './orders.repository';

@Injectable()
export class OrdersService {
  constructor(
    @Inject(ORDERS_REPOSITORY)
    private readonly ordersRepository: OrdersRepository,
    @Inject(PAYMENT_COMMAND_PUBLISHER)
    private readonly commandPublisher: PaymentCommandPublisher,
    private readonly statusStream: OrderStatusStreamService,
  ) {}

  async createOrder(input: CreateOrderInput) {
    const orderNumber = randomUUID();
    const reserveId = randomUUID();

    this.ordersRepository.save({
      orderNumber,
      reserveId,
      productId: input.productId,
      amount: input.amount,
      customerEmail: input.customerEmail,
      status: 'payment_pending',
    });

    this.statusStream.register(orderNumber);

    await this.commandPublisher.publishPaymentRequested({
      reserveId,
      orderNumber,
      amount: input.amount,
      customerEmail: input.customerEmail,
    });

    return { orderNumber, status: 'payment_pending' as const };
  }

  getOrder(orderNumber: string): Order {
    const order = this.ordersRepository.findByOrderNumber(orderNumber);
    if (!order) {
      throw new NotFoundException(`Order ${orderNumber} not found`);
    }
    return order;
  }

  applyPaymentResult(
    orderNumber: string,
    status: Extract<OrderStatus, 'payment_succeeded' | 'payment_failed'>,
  ): void {
    const order = this.ordersRepository.updateStatus(orderNumber, status);
    if (!order) return;
    this.statusStream.push(orderNumber, status);
  }
}
```

### Step 5.8 — HTTP controllers

- [x] Replace `services/api-gateway/src/orders/orders.controller.ts`:

```typescript
import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { CreateOrderSchema, OrderNumberParamSchema } from './create-order.schema';
import { OrdersService } from './orders.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  createOrder(@Body() body: unknown) {
    const input = CreateOrderSchema.parse(body);
    return this.ordersService.createOrder(input);
  }

  @Get(':orderNumber')
  getOrder(@Param('orderNumber') orderNumber: string) {
    OrderNumberParamSchema.parse(orderNumber);
    return this.ordersService.getOrder(orderNumber);
  }
}
```

- [x] Replace `services/api-gateway/src/orders/orders-events.controller.ts`:

```typescript
import { Controller, Param, Sse } from '@nestjs/common';
import { OrderNumberParamSchema } from './create-order.schema';
import { OrdersService } from './orders.service';
import { OrderStatusStreamService } from './order-status-stream.service';

@Controller('orders')
export class OrdersEventsController {
  constructor(
    private readonly ordersService: OrdersService,
    private readonly statusStream: OrderStatusStreamService,
  ) {}

  @Sse(':orderNumber/events')
  stream(@Param('orderNumber') orderNumber: string) {
    OrderNumberParamSchema.parse(orderNumber);
    this.ordersService.getOrder(orderNumber);
    return this.statusStream.watch(orderNumber);
  }
}
```

### Step 5.9 — Kafka handler (`payment-events.handler.ts`)

- [x] Replace `services/api-gateway/src/payment-events/payment-events.controller.ts` → rename file to `payment-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  PaymentFailedSchema,
  PaymentSucceededSchema,
  TOPICS,
} from '@eda/contracts';
import { OrdersService } from '../orders/orders.service';

@Controller()
export class PaymentEventsHandler {
  private readonly logger = new Logger(PaymentEventsHandler.name);

  constructor(private readonly ordersService: OrdersService) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);
    this.logger.log(`Payment succeeded for order ${event.orderNumber}`);
    this.ordersService.applyPaymentResult(
      event.orderNumber,
      'payment_succeeded',
    );
  }

  @EventPattern(TOPICS.PAYMENT_FAILED)
  handlePaymentFailed(@Payload() payload: unknown) {
    const event = PaymentFailedSchema.parse(payload);
    this.logger.log(`Payment failed for order ${event.orderNumber}`);
    this.ordersService.applyPaymentResult(event.orderNumber, 'payment_failed');
  }
}
```

### Step 5.10 — Wire modules (hybrid app)

- [x] Replace `services/api-gateway/src/orders/orders.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { EXCHANGES } from '@eda/contracts';
import { requireEnv } from '../common/env';
import { RabbitMqPaymentCommandPublisher } from '../messaging/rabbitmq-payment-command.publisher';
import { PAYMENT_COMMAND_PUBLISHER } from '../messaging/payment-command.publisher';
import { PaymentEventsHandler } from '../payment-events/payment-events.handler';
import { InMemoryOrdersRepository } from './in-memory-orders.repository';
import { OrderStatusStreamService } from './order-status-stream.service';
import { OrdersEventsController } from './orders-events.controller';
import { OrdersController } from './orders.controller';
import { ORDERS_REPOSITORY } from './orders.repository';
import { OrdersService } from './orders.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'RABBITMQ_COMMANDS',
        transport: Transport.RMQ,
        options: {
          urls: [requireEnv('RABBITMQ_URL')],
          queue: 'orders.payment.requested',
          queueOptions: { durable: true },
          wildcards: true,
          exchange: EXCHANGES.COMMANDS,
          exchangeType: 'topic',
        },
      },
    ]),
  ],
  controllers: [
    OrdersController,
    OrdersEventsController,
    PaymentEventsHandler,
  ],
  providers: [
    OrdersService,
    OrderStatusStreamService,
    {
      provide: ORDERS_REPOSITORY,
      useClass: InMemoryOrdersRepository,
    },
    {
      provide: PAYMENT_COMMAND_PUBLISHER,
      useClass: RabbitMqPaymentCommandPublisher,
    },
  ],
})
export class OrdersModule {}
```

- [x] Replace `services/api-gateway/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { OrdersModule } from './orders/orders.module';

@Module({
  imports: [OrdersModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Replace `services/api-gateway/src/main.ts`:

```typescript
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'api-gateway',
        brokers: requireEnv('KAFKA_BROKERS').split(','),
      },
      consumer: { groupId: 'api-gateway' },
      subscribe: { fromBeginning: false },
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port, '0.0.0.0');
  console.log(`api-gateway listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 5.11 — Build and run

- [x] From **`services/api-gateway/`**:

```bash
pnpm build
pnpm start:dev
```

**Expected:** Build succeeds; console shows `api-gateway listening on 3000`.

### Checkpoint

- `services/api-gateway` builds and runs from its own folder.
- Flow: `POST /orders` → Zod validates body → RabbitMQ command; Kafka `payment_succeeded` / `payment_failed` → Zod validates payload → repository update → SSE push.

### Suggested commit

```bash
git add packages/contracts infra/kafka/init-topics.sh services/api-gateway
git commit -m "feat: add api-gateway with zod validation, repository, and messaging ports"
```

---

## Phase 6 — Payment Service

**Goal:** Consume `orders.payment.requested`, call Stripe via gateway, publish `payment_succeeded` / `payment_failed` to Kafka. Hybrid app (RabbitMQ consumer + HTTP webhook).

**Port:** `3010` | **Folder:** `services/payment/`

```text
services/payment/src/
├── payment/
│   payment.service.ts
│   payment.module.ts
├── payment-consumer/
│   payment-consumer.handler.ts
├── webhooks/
│   webhooks.controller.ts
├── gateways/
│   payment.gateway.ts
│   stripe-payment.gateway.ts
└── messaging/
    domain-event.publisher.ts
    kafka-domain-event.publisher.ts
```

### Step 6.1 — Scaffold standalone project

- [x] Scaffold and configure (same pattern as [Phase 5 Step 5.1](#step-51--scaffold-standalone-project), including [Biome swap](#biome-instead-of-eslintprettier-every-microservice)):

```bash
cd services
npx nest new payment --package-manager pnpm --strict --skip-git
cd payment
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
cp ../api-gateway/biome.json ./biome.json
# update format/lint scripts in package.json — same as api-gateway
pnpm add @nestjs/platform-fastify @nestjs/microservices amqplib amqp-connection-manager kafkajs dotenv zod
pnpm add -D @types/amqplib
pnpm add @eda/contracts@file:../../packages/contracts
cp -r ../api-gateway/src/common ./src/common
```

- [x] Create `services/payment/.env`:

```bash
PORT=3010
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
KAFKA_BROKERS=localhost:9094
STRIPE_MOCK_URL=http://localhost:3001
```

- [x] Scaffold modules (from **`services/payment/`**):

```bash
npx nest g module payment --no-spec
npx nest g service payment --no-spec
npx nest g controller payment-consumer --no-spec
npx nest g controller webhooks --no-spec
mkdir -p src/gateways src/messaging
```

### Step 6.1.1 — Stripe webhook schema (`@eda/contracts`)

- [x] Create `packages/contracts/src/events/stripe-webhook.ts`:

```typescript
import { z } from 'zod';

export const StripeWebhookDataSchema = z.object({
  orderNumber: z.string().uuid(),
  amount: z.number().positive(),
  reserveId: z.string().uuid(),
  customerEmail: z.string().email(),
});

export const StripeWebhookSchema = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('payment_intent.succeeded'),
    data: StripeWebhookDataSchema,
  }),
  z.object({
    type: z.literal('payment_intent.payment_failed'),
    data: StripeWebhookDataSchema,
  }),
]);

export type StripeWebhook = z.infer<typeof StripeWebhookSchema>;
```

- [x] Export from `packages/contracts/src/index.ts`:

```typescript
export * from './events/stripe-webhook';
```

- [x] Rebuild contracts:

```bash
pnpm --filter @eda/contracts build
```

### Step 6.2 — Payment gateway  

- [x] Create `services/payment/src/gateways/payment.gateway.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts';

export const PAYMENT_GATEWAY = Symbol('PAYMENT_GATEWAY');

export interface PaymentGateway {
  createPaymentIntent(input: PaymentRequested): Promise<void>;
}
```

- [x] Create `services/payment/src/gateways/stripe-payment.gateway.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { PaymentRequested } from '@eda/contracts';
import { requireEnv } from '../common/env';
import { PaymentGateway } from './payment.gateway';

@Injectable()
export class StripePaymentGateway implements PaymentGateway {
  private readonly baseUrl = requireEnv('STRIPE_MOCK_URL');

  async createPaymentIntent(input: PaymentRequested): Promise<void> {
    const response = await fetch(`${this.baseUrl}/v1/payment-intents`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        orderNumber: input.orderNumber,
        amount: input.amount,
        reserveId: input.reserveId,
        customerEmail: input.customerEmail,
      }),
    });

    if (!response.ok) {
      throw new Error(`Stripe mock returned HTTP ${response.status}`);
    }
  }
}
```

### Step 6.3 — Domain event publisher

- [x] Create `services/payment/src/messaging/domain-event.publisher.ts`:

```typescript
import { PaymentFailed, PaymentSucceeded } from '@eda/contracts';

export const DOMAIN_EVENT_PUBLISHER = Symbol('DOMAIN_EVENT_PUBLISHER');

export interface DomainEventPublisher {
  publishPaymentSucceeded(event: PaymentSucceeded): Promise<void>;
  publishPaymentFailed(event: PaymentFailed): Promise<void>;
}
```

- [x] Create `services/payment/src/messaging/kafka-domain-event.publisher.ts`:

```typescript
import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientKafka, ClientKafka } from '@nestjs/microservices';
import {
  PaymentFailed,
  PaymentFailedSchema,
  PaymentSucceeded,
  PaymentSucceededSchema,
  TOPICS,
} from '@eda/contracts';
import { firstValueFrom } from 'rxjs';
import { DomainEventPublisher } from './domain-event.publisher';

@Injectable()
export class KafkaDomainEventPublisher
  implements DomainEventPublisher, OnModuleInit, OnModuleDestroy
{
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await (this.kafka).connect();
  }

  async publishPaymentSucceeded(event: PaymentSucceeded): Promise<void> {
    const payload = PaymentSucceededSchema.parse(event);
    await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_SUCCEEDED, payload));
  }

  async publishPaymentFailed(event: PaymentFailed): Promise<void> {
    const payload = PaymentFailedSchema.parse(event);
    await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_FAILED, payload));
  }

  async onModuleDestroy() {
    await (this.kafka).close();
  }
}
```

### Step 6.4 — Payment service

- [x] Replace `services/payment/src/payment/payment.service.ts`:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import {
  PaymentRequested,
  PaymentSucceededSchema,
  StripeWebhook,
} from '@eda/contracts';
import {
  DOMAIN_EVENT_PUBLISHER,
  DomainEventPublisher,
} from '../messaging/domain-event.publisher';
import { PAYMENT_GATEWAY, PaymentGateway } from '../gateways/payment.gateway';

@Injectable()
export class PaymentService {
  constructor(
    @Inject(PAYMENT_GATEWAY) private readonly paymentGateway: PaymentGateway,
    @Inject(DOMAIN_EVENT_PUBLISHER)
    private readonly events: DomainEventPublisher,
  ) {}

  async processPaymentRequested(data: PaymentRequested): Promise<void> {
    await this.paymentGateway.createPaymentIntent(data);
  }

  async publishPaymentFailed(orderNumber: string, reason: string): Promise<void> {
    await this.events.publishPaymentFailed({ orderNumber, reason });
  }

  async handleStripeWebhook(body: StripeWebhook): Promise<void> {
    if (body.type === 'payment_intent.succeeded') {
      await this.events.publishPaymentSucceeded(
        PaymentSucceededSchema.parse({
          reserveId: body.data.reserveId,
          value: body.data.amount,
          customerInfo: { email: body.data.customerEmail },
          orderNumber: body.data.orderNumber,
        }),
      );
      return;
    }

    if (body.type === 'payment_intent.payment_failed') {
      await this.events.publishPaymentFailed({
        orderNumber: body.data.orderNumber,
        reason: 'payment_intent.payment_failed',
      });
    }
  }
}
```

### Step 6.5 — RabbitMQ handler (thin)

- [x] Replace `services/payment/src/payment-consumer/payment-consumer.controller.ts` → rename to `payment-consumer.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';
import { PaymentRequestedSchema, ROUTING_KEYS } from '@eda/contracts';
import { IdempotencyStore } from '../common/idempotency.store';
import { PaymentService } from '../payment/payment.service';

@Controller()
export class PaymentConsumerHandler {
  private readonly logger = new Logger(PaymentConsumerHandler.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly paymentService: PaymentService,
  ) {}

  @EventPattern(ROUTING_KEYS.PAYMENT_REQUESTED)
  async handlePaymentRequested(
    @Payload() payload: unknown,
    @Ctx() context: RmqContext,
  ) {
    const channel = context.getChannelRef();
    const msg = context.getMessage();

    try {
      const data = PaymentRequestedSchema.parse(payload);

      if (this.idempotency.isDuplicate(data.orderNumber)) {
        this.logger.warn(`Duplicate orders.payment.requested: ${data.orderNumber}`);
        channel.ack(msg);
        return;
      }

      this.logger.log(`Processing payment for order ${data.orderNumber}`);

      try {
        await this.paymentService.processPaymentRequested(data);
      } catch (err) {
        this.logger.error('Stripe call failed', err);
        await this.paymentService.publishPaymentFailed(
          data.orderNumber,
          'stripe_error',
        );
      }

      this.idempotency.markProcessed(data.orderNumber);
      channel.ack(msg);
    } catch (err) {
      this.logger.error('Failed to process orders.payment.requested', err);
      channel.nack(msg, false, false);
    }
  }
}
```

### Step 6.6 — Webhook controller (thin)

- [x] Replace `services/payment/src/webhooks/webhooks.controller.ts`:

```typescript
import { Body, Controller, Post } from '@nestjs/common';
import { StripeWebhookSchema } from '@eda/contracts';
import { PaymentService } from '../payment/payment.service';

@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly paymentService: PaymentService) {}

  @Post('stripe')
  async stripeWebhook(@Body() body: unknown) {
    const event = StripeWebhookSchema.parse(body);
    await this.paymentService.handleStripeWebhook(event);
    return { received: true };
  }
}
```

### Step 6.7 — Wire modules (hybrid app)

- [x] Replace `services/payment/src/payment/payment.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { EXCHANGES } from '@eda/contracts';
import { IdempotencyStore } from '../common/idempotency.store';
import { requireEnv } from '../common/env';
import { StripePaymentGateway } from '../gateways/stripe-payment.gateway';
import { PAYMENT_GATEWAY } from '../gateways/payment.gateway';
import { KafkaDomainEventPublisher } from '../messaging/kafka-domain-event.publisher';
import { DOMAIN_EVENT_PUBLISHER } from '../messaging/domain-event.publisher';
import { PaymentConsumerHandler } from '../payment-consumer/payment-consumer.handler';
import { WebhooksController } from '../webhooks/webhooks.controller';
import { PaymentService } from './payment.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'payment-service',
            brokers: requireEnv('KAFKA_BROKERS').split(','),
          },
          producer: { allowAutoTopicCreation: false },
        },
      },
    ]),
  ],
  controllers: [PaymentConsumerHandler, WebhooksController],
  providers: [
    IdempotencyStore,
    PaymentService,
    {
      provide: PAYMENT_GATEWAY,
      useClass: StripePaymentGateway,
    },
    {
      provide: DOMAIN_EVENT_PUBLISHER,
      useClass: KafkaDomainEventPublisher,
    },
  ],
})
export class PaymentModule {}
```

- [x] Replace `services/payment/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { PaymentModule } from './payment/payment.module';

@Module({
  imports: [PaymentModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Replace `services/payment/src/main.ts`:

```typescript
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { EXCHANGES } from '@eda/contracts';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [requireEnv('RABBITMQ_URL')],
      queue: 'orders.payment.requested',
      noAck: false,
      prefetchCount: 1,
      queueOptions: { durable: true },
      wildcards: true,
      exchange: EXCHANGES.COMMANDS,
      exchangeType: 'topic',
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3010);
  await app.listen(port, '0.0.0.0');
  console.log(`💰 payment-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 6.8 — Build

- [ ] From **`services/payment/`**:

```bash
pnpm build
```

**Expected:** Build succeeds.

### Checkpoint

- Payment service builds from its own folder.
- Flow: RabbitMQ handler → Zod validates command → `PaymentService` → `PaymentGateway`; Stripe webhook → Zod validates body → `DomainEventPublisher` → Zod validates event → Kafka.

### Suggested commit

```bash
git add packages/contracts services/payment
git commit -m "feat: add payment service with zod validation, gateway, and event publisher ports"
```

---

## Phase 7 — Availability Service

**Goal:** Consume `orders.payment.succeeded`, confirm inventory reservation. Hybrid app (HTTP health + Kafka consumer).

**Port:** `3020` | **Folder:** `services/availability/`

```text
services/availability/src/
├── inventory/
│   inventory.repository.ts
│   in-memory-inventory.repository.ts
│   inventory.service.ts
├── payment-events/
│   payment-events.handler.ts
└── availability.module.ts
```

### Step 7.1 — Scaffold standalone project

- [x] Scaffold and configure (same pattern as [Phase 5 Step 5.1](#step-51--scaffold-standalone-project), including [Biome swap](#biome-instead-of-eslintprettier-every-microservice)):

```bash
cd services
npx nest new availability --package-manager pnpm --strict --skip-git
cd availability
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
pnpm add @nestjs/platform-fastify @nestjs/microservices kafkajs dotenv zod
pnpm add @eda/contracts@file:../../packages/contracts
cp -r ../api-gateway/src/common ./src/common
cp ../api-gateway/biome.json ./biome.json
# update format/lint scripts in package.json — same as api-gateway
```

- [x] Create `services/availability/.env`:

```bash
PORT=3020
KAFKA_BROKERS=localhost:9094
```

- [x] Scaffold modules (from **`services/availability/`**):

```bash
npx nest g module availability --no-spec
npx nest g service inventory --no-spec
npx nest g controller payment-events --no-spec
mkdir -p src/inventory
```

### Step 7.2 — Inventory repository

- [x] Create `services/availability/src/inventory/inventory.repository.ts`:

```typescript
export const INVENTORY_REPOSITORY = Symbol('INVENTORY_REPOSITORY');

export interface InventoryRepository {
  confirmReservation(reserveId: string): void;
}
```

- [x] Create `services/availability/src/inventory/in-memory-inventory.repository.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { InventoryRepository } from './inventory.repository';

@Injectable()
export class InMemoryInventoryRepository implements InventoryRepository {
  private readonly logger = new Logger(InMemoryInventoryRepository.name);
  private stock = 100;

  confirmReservation(reserveId: string): void {
    this.stock -= 1;
    this.logger.log(
      `Confirmed reservation ${reserveId}. Remaining stock: ${this.stock}`,
    );
  }
}
```

### Step 7.3 — Inventory service

- [x] Create `services/availability/src/inventory/inventory.service.ts` (delete scaffolded `src/inventory.service.ts` if present):

```typescript
import { Inject, Injectable } from '@nestjs/common';
import {
  INVENTORY_REPOSITORY,
  InventoryRepository,
} from './inventory.repository';

@Injectable()
export class InventoryService {
  constructor(
    @Inject(INVENTORY_REPOSITORY)
    private readonly inventoryRepository: InventoryRepository,
  ) {}

  confirmReservation(reserveId: string): void {
    this.inventoryRepository.confirmReservation(reserveId);
  }
}
```

### Step 7.4 — Kafka handler (thin)

- [x] Replace `services/availability/src/payment-events/payment-events.controller.ts` → rename to `payment-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '../common/idempotency.store';
import { InventoryService } from '../inventory/inventory.service';

@Controller()
export class PaymentEventsHandler {
  private readonly logger = new Logger(PaymentEventsHandler.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly inventoryService: InventoryService,
  ) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.orderNumber)) {
      this.logger.warn(`Duplicate orders.payment.succeeded: ${event.orderNumber}`);
      return;
    }

    this.inventoryService.confirmReservation(event.reserveId);
    this.idempotency.markProcessed(event.orderNumber);
  }
}
```

### Step 7.5 — Wire modules and hybrid main

- [x] Replace `services/availability/src/availability.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { IdempotencyStore } from './common/idempotency.store';
import { InMemoryInventoryRepository } from './inventory/in-memory-inventory.repository';
import { INVENTORY_REPOSITORY } from './inventory/inventory.repository';
import { InventoryService } from './inventory/inventory.service';
import { PaymentEventsHandler } from './payment-events/payment-events.handler';

@Module({
  controllers: [PaymentEventsHandler],
  providers: [
    IdempotencyStore,
    InventoryService,
    {
      provide: INVENTORY_REPOSITORY,
      useClass: InMemoryInventoryRepository,
    },
  ],
})
export class AvailabilityModule {}
```

- [x] Create `services/availability/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { AvailabilityModule } from './availability.module';

@Module({
  imports: [AvailabilityModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Create `services/availability/src/main.ts`:

```typescript
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'availability-service',
        brokers: requireEnv('KAFKA_BROKERS').split(','),
      },
      consumer: { groupId: 'availability-service' },
      subscribe: { fromBeginning: false },
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3020);
  await app.listen(port, '0.0.0.0');
  console.log(`availability-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 7.6 — Build

- [x] From **`services/availability/`**:

```bash
pnpm build
```

**Expected:** Build succeeds.

### Checkpoint

- Availability service builds from its own folder.
- Kafka handler delegates to `InventoryService` → `InventoryRepository`.

### Suggested commit

```bash
git add services/availability
git commit -m "feat: add availability service with inventory repository"
```

---

## Phase 8 — Analytics Service

**Goal:** Project Kafka events and email commands into an in-memory store; expose via HTTP for debugging. Hybrid app with Kafka + RabbitMQ consumers.

**Port:** `3030` | **Folder:** `services/analytics/`

```text
services/analytics/src/
├── events/
│   events.repository.ts
│   in-memory-events.repository.ts
│   events.service.ts
│   events.controller.ts
├── kafka-events/
│   kafka-events.handler.ts
└── rabbitmq-events/
    rabbitmq-events.handler.ts
```

### Step 8.1 — Scaffold standalone project

- [x] Scaffold and configure (same pattern as [Phase 5 Step 5.1](#step-51--scaffold-standalone-project), including [Biome swap](#biome-instead-of-eslintprettier-every-microservice)):

```bash
cd services
npx nest new analytics --package-manager pnpm --strict --skip-git
cd analytics
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
pnpm add @nestjs/platform-fastify @nestjs/microservices kafkajs dotenv zod
pnpm add @eda/contracts@file:../../packages/contracts
cp -r ../api-gateway/src/common ./src/common
cp ../api-gateway/biome.json ./biome.json
# update format/lint scripts in package.json — same as api-gateway
```

- [x] Create `services/analytics/.env`:

```bash
PORT=3030
KAFKA_BROKERS=localhost:9094
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
```

- [x] Scaffold modules (from **`services/analytics/`**):

```bash
npx nest g module analytics --no-spec
npx nest g controller events --no-spec
npx nest g controller kafka-events --no-spec
npx nest g service events --no-spec
mkdir -p src/events src/kafka-events src/rabbitmq-events
```

### Step 8.2 — Events repository + service + HTTP controller

- [x] Create `services/analytics/src/events/events.repository.ts`:

```typescript
export interface StoredEvent {
  type: string;
  payload: unknown;
  at: string;
}

export const EVENTS_REPOSITORY = Symbol('EVENTS_REPOSITORY');

export interface EventsRepository {
  append(type: string, payload: unknown): void;
  list(): StoredEvent[];
}
```

- [x] Create `services/analytics/src/events/in-memory-events.repository.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { EventsRepository, StoredEvent } from './events.repository';

@Injectable()
export class InMemoryEventsRepository implements EventsRepository {
  private readonly events: StoredEvent[] = [];

  append(type: string, payload: unknown): void {
    this.events.push({ type, payload, at: new Date().toISOString() });
  }

  list(): StoredEvent[] {
    return this.events;
  }
}
```

- [x] Create `services/analytics/src/events/events.service.ts` (delete scaffolded `src/events.service.ts` if present):

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { EVENTS_REPOSITORY, EventsRepository } from './events.repository';

@Injectable()
export class EventsService {
  constructor(
    @Inject(EVENTS_REPOSITORY)
    private readonly eventsRepository: EventsRepository,
  ) {}

  record(type: string, payload: unknown): void {
    this.eventsRepository.append(type, payload);
  }

  list() {
    return this.eventsRepository.list();
  }
}
```

- [x] Create `services/analytics/src/events/events.controller.ts` (delete scaffolded `src/events.controller.ts` if present):

```typescript
import { Controller, Get } from '@nestjs/common';
import { EventsService } from './events.service';

@Controller('events')
export class EventsController {
  constructor(private readonly eventsService: EventsService) {}

  @Get()
  list() {
    return this.eventsService.list();
  }
}
```

### Step 8.3 — Kafka handlers (thin)

- [x] Replace `services/analytics/src/kafka-events/kafka-events.controller.ts` → rename to `kafka-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  InvoiceCreatedSchema,
  PaymentSucceededSchema,
  TOPICS,
} from '@eda/contracts';
import { EventsService } from '../events/events.service';

@Controller()
export class KafkaEventsHandler {
  private readonly logger = new Logger(KafkaEventsHandler.name);

  constructor(private readonly eventsService: EventsService) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);
    this.logger.log(`Recorded orders.payment.succeeded: ${event.orderNumber}`);
    this.eventsService.record('orders.payment.succeeded', event);
  }

  @EventPattern(TOPICS.INVOICE_CREATED)
  handleInvoiceCreated(@Payload() payload: unknown) {
    const event = InvoiceCreatedSchema.parse(payload);
    this.logger.log(`Recorded billing.invoice.created: ${event.invoiceId}`);
    this.eventsService.record('billing.invoice.created', event);
  }
}
```

### Step 8.4 — RabbitMQ handlers (thin)

- [x] Create `services/analytics/src/rabbitmq-events/rabbitmq-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  EmailDeliveredSchema,
  EmailFailedSchema,
  ROUTING_KEYS,
} from '@eda/contracts';
import { EventsService } from '../events/events.service';

@Controller()
export class RabbitMqEventsHandler {
  private readonly logger = new Logger(RabbitMqEventsHandler.name);

  constructor(private readonly eventsService: EventsService) {}

  @EventPattern(ROUTING_KEYS.EMAIL_DELIVERED)
  handleEmailDelivered(@Payload() payload: unknown) {
    const event = EmailDeliveredSchema.parse(payload);
    this.logger.log(
      `Recorded notifications.email.delivered: ${event.invoiceId}`,
    );
    this.eventsService.record('notifications.email.delivered', event);
  }

  @EventPattern(ROUTING_KEYS.EMAIL_FAILED)
  handleEmailFailed(@Payload() payload: unknown) {
    const event = EmailFailedSchema.parse(payload);
    this.logger.log(`Recorded notifications.email.failed: ${event.invoiceId}`);
    this.eventsService.record('notifications.email.failed', event);
  }
}
```

### Step 8.5 — Wire modules and hybrid main

- [x] Replace `services/analytics/src/analytics.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { EventsController } from './events/events.controller';
import { InMemoryEventsRepository } from './events/in-memory-events.repository';
import { EVENTS_REPOSITORY } from './events/events.repository';
import { EventsService } from './events/events.service';
import { KafkaEventsHandler } from './kafka-events/kafka-events.handler';
import { RabbitMqEventsHandler } from './rabbitmq-events/rabbitmq-events.handler';

@Module({
  controllers: [EventsController, KafkaEventsHandler, RabbitMqEventsHandler],
  providers: [
    EventsService,
    {
      provide: EVENTS_REPOSITORY,
      useClass: InMemoryEventsRepository,
    },
  ],
})
export class AnalyticsModule {}
```

- [x] Create `services/analytics/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { AnalyticsModule } from './analytics.module';

@Module({
  imports: [AnalyticsModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Replace `services/analytics/src/main.ts`:

```typescript
import 'dotenv/config';
import { EXCHANGES } from '@eda/contracts';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'analytics-service',
        brokers: requireEnv('KAFKA_BROKERS').split(','),
      },
      consumer: { groupId: 'analytics-service' },
      subscribe: { fromBeginning: false },
    },
  });

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [requireEnv('RABBITMQ_URL')],
      queue: 'notifications.email.delivered',
      noAssert: true,
      noAck: false,
      prefetchCount: 1,
      queueOptions: { durable: true },
      wildcards: true,
      exchange: EXCHANGES.COMMANDS,
      exchangeType: 'topic',
    },
  });

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [requireEnv('RABBITMQ_URL')],
      queue: 'notifications.email.failed',
      noAssert: true,
      noAck: false,
      prefetchCount: 1,
      queueOptions: { durable: true },
      wildcards: true,
      exchange: EXCHANGES.COMMANDS,
      exchangeType: 'topic',
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3030);
  await app.listen(port, '0.0.0.0');
  console.log(`analytics-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 8.6 — Build

- [ ] From **`services/analytics/`**:

```bash
pnpm build
```

**Expected:** Build succeeds.

### Checkpoint

- Analytics service builds from its own folder.
- Kafka handlers delegate to `EventsService` → `EventsRepository`.
- RabbitMQ handlers record `notifications.email.delivered` and `notifications.email.failed`.

### Suggested commit

```bash
git add services/analytics
git commit -m "feat: add analytics RabbitMQ handlers for email commands"
```

---

## Phase 9 — Invoice Service

**Goal:** Consume `orders.payment.succeeded`, create invoice, publish `billing.invoice.created`. Hybrid app (Kafka consumer + producer).

**Port:** `3040` | **Folder:** `services/invoice/`

```text
services/invoice/src/
├── invoice/
│   invoice.service.ts
│   invoice.module.ts
├── payment-events/
│   payment-events.handler.ts
└── messaging/
    domain-event.publisher.ts
    kafka-domain-event.publisher.ts
```

### Step 9.1 — Scaffold standalone project

- [x] Scaffold and configure (same pattern as [Phase 5 Step 5.1](#step-51--scaffold-standalone-project), including [Biome swap](#biome-instead-of-eslintprettier-every-microservice)):

```bash
cd services
npx nest new invoice --package-manager pnpm --strict --skip-git
cd invoice
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
pnpm add @nestjs/platform-fastify @nestjs/microservices kafkajs dotenv zod
pnpm add @eda/contracts@file:../../packages/contracts
cp -r ../api-gateway/src/common ./src/common
cp ../api-gateway/biome.json ./biome.json
# update format/lint scripts in package.json — same as api-gateway
```

- [x] Create `services/invoice/.env`:

```bash
PORT=3040
KAFKA_BROKERS=localhost:9094
```

- [x] Scaffold modules (from **`services/invoice/`**):

```bash
npx nest g module invoice --no-spec
npx nest g service invoice --no-spec
npx nest g controller payment-events --no-spec
mkdir -p src/messaging src/payment-events
```

### Step 9.2 — Domain event publisher

- [x] Create `services/invoice/src/messaging/domain-event.publisher.ts`:

```typescript
import { InvoiceCreated } from '@eda/contracts';

export const DOMAIN_EVENT_PUBLISHER = Symbol('DOMAIN_EVENT_PUBLISHER');

export interface DomainEventPublisher {
  publishInvoiceCreated(event: InvoiceCreated): Promise<void>;
}
```

- [x] Create `services/invoice/src/messaging/kafka-domain-event.publisher.ts`:

```typescript
import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientKafka, ClientKafka } from '@nestjs/microservices';
import { InvoiceCreated, InvoiceCreatedSchema, TOPICS } from '@eda/contracts';
import { firstValueFrom } from 'rxjs';
import { DomainEventPublisher } from './domain-event.publisher';

@Injectable()
export class KafkaDomainEventPublisher
  implements DomainEventPublisher, OnModuleInit, OnModuleDestroy
{
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await (this.kafka).connect();
  }

  async publishInvoiceCreated(event: InvoiceCreated): Promise<void> {
    const payload = InvoiceCreatedSchema.parse(event);
    await firstValueFrom(this.kafka.emit(TOPICS.INVOICE_CREATED, payload));
  }

  async onModuleDestroy() {
    await (this.kafka).close();
  }
}
```

### Step 9.3 — Invoice service

- [x] Replace `services/invoice/src/invoice/invoice.service.ts`:

```typescript
import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  InvoiceCreatedSchema,
  PaymentSucceeded,
} from '@eda/contracts';
import { randomUUID } from 'crypto';
import {
  DOMAIN_EVENT_PUBLISHER,
  DomainEventPublisher,
} from '../messaging/domain-event.publisher';

@Injectable()
export class InvoiceService {
  private readonly logger = new Logger(InvoiceService.name);

  constructor(
    @Inject(DOMAIN_EVENT_PUBLISHER)
    private readonly events: DomainEventPublisher,
  ) {}

  async createFromPayment(event: PaymentSucceeded): Promise<void> {
    const invoiceId = randomUUID();
    this.logger.log(`Creating invoice ${invoiceId} for order ${event.orderNumber}`);

    await this.events.publishInvoiceCreated(
      InvoiceCreatedSchema.parse({
        invoiceId,
        value: event.value,
        customerInfo: event.customerInfo,
        orderNumber: event.orderNumber,
      }),
    );
  }
}
```

### Step 9.4 — Kafka handler (thin)

- [x] Replace `services/invoice/src/payment-events/payment-events.controller.ts` → rename to `payment-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '../common/idempotency.store';
import { InvoiceService } from '../invoice/invoice.service';

@Controller()
export class PaymentEventsHandler {
  private readonly logger = new Logger(PaymentEventsHandler.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly invoiceService: InvoiceService,
  ) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  async handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.orderNumber)) {
      this.logger.warn(`Duplicate orders.payment.succeeded: ${event.orderNumber}`);
      return;
    }

    await this.invoiceService.createFromPayment(event);
    this.idempotency.markProcessed(event.orderNumber);
  }
}
```

### Step 9.5 — Wire modules and hybrid main

- [x] Replace `services/invoice/src/invoice/invoice.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { IdempotencyStore } from '../common/idempotency.store';
import { requireEnv } from '../common/env';
import { KafkaDomainEventPublisher } from '../messaging/kafka-domain-event.publisher';
import { DOMAIN_EVENT_PUBLISHER } from '../messaging/domain-event.publisher';
import { PaymentEventsHandler } from '../payment-events/payment-events.handler';
import { InvoiceService } from './invoice.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'invoice-service',
            brokers: requireEnv('KAFKA_BROKERS').split(','),
          },
          producer: { allowAutoTopicCreation: false },
        },
      },
    ]),
  ],
  controllers: [PaymentEventsHandler],
  providers: [
    IdempotencyStore,
    InvoiceService,
    {
      provide: DOMAIN_EVENT_PUBLISHER,
      useClass: KafkaDomainEventPublisher,
    },
  ],
})
export class InvoiceModule {}
```

- [x] Create `services/invoice/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { InvoiceModule } from './invoice/invoice.module';

@Module({
  imports: [InvoiceModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Create `services/invoice/src/main.ts`:

```typescript
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'invoice-service',
        brokers: requireEnv('KAFKA_BROKERS').split(','),
      },
      consumer: { groupId: 'invoice-service' },
      subscribe: { fromBeginning: false },
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3040);
  await app.listen(port, '0.0.0.0');
  console.log(`invoice-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 9.6 — Build

- [x] From **`services/invoice/`**:

```bash
pnpm build
```

**Expected:** Build succeeds.

### Checkpoint

- Invoice service builds from its own folder.
- Kafka handler → `InvoiceService` → `DomainEventPublisher`.

### Suggested commit

```bash
git add services/invoice
git commit -m "feat: add invoice service with event publisher port"
```

---

## Phase 10 — Notification Service

**Goal:** Consume `billing.invoice.created`, send email via SendGrid gateway, receive Event Webhook, emit RabbitMQ commands to Analytics.

**Port:** `3050` | **Folder:** `services/notification/`

```text
services/notification/src/
├── notification/
│   notification.service.ts
│   notification.module.ts
├── invoice-events/
│   invoice-events.handler.ts
├── webhooks/
│   webhooks.controller.ts
├── gateways/
│   email.gateway.ts
│   sendgrid-email.gateway.ts
└── messaging/
    analytics-command.publisher.ts
    rabbitmq-analytics-command.publisher.ts
```

### Step 10.1 — Scaffold standalone project

- [x] Scaffold and configure (same pattern as [Phase 5 Step 5.1](#step-51--scaffold-standalone-project), including [Biome swap](#biome-instead-of-eslintprettier-every-microservice)):

```bash
cd services
npx nest new notification --package-manager pnpm --strict --skip-git
cd notification
rm .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
pnpm add @nestjs/platform-fastify @nestjs/microservices kafkajs dotenv zod
pnpm add @eda/contracts@file:../../packages/contracts
cp -r ../api-gateway/src/common ./src/common
cp ../api-gateway/biome.json ./biome.json
# update format/lint scripts in package.json — same as api-gateway
```

- [x] Create `services/notification/.env`:

```bash
PORT=3050
KAFKA_BROKERS=localhost:9094
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
SENDGRID_MOCK_URL=http://localhost:3002
```

- [x] Scaffold modules (from **`services/notification/`**):

```bash
npx nest g module notification --no-spec
npx nest g service notification --no-spec
npx nest g controller invoice-events --no-spec
npx nest g controller webhooks --no-spec
mkdir -p src/gateways src/invoice-events src/webhooks src/messaging
```

### Step 10.2 — Email gateway

- [x] Create `services/notification/src/gateways/email.gateway.ts`:

```typescript
export const EMAIL_GATEWAY = Symbol('EMAIL_GATEWAY');

export interface EmailGateway {
  sendInvoiceEmail(
    email: string,
    invoiceId: string,
    orderNumber: string,
  ): Promise<void>;
}
```

- [x] Replace `services/notification/src/gateways/sendgrid-email.gateway.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { requireEnv } from '../common/env';
import { EmailGateway } from './email.gateway';

@Injectable()
export class SendGridEmailGateway implements EmailGateway {
  private readonly logger = new Logger(SendGridEmailGateway.name);
  private readonly baseUrl = requireEnv('SENDGRID_MOCK_URL');

  async sendInvoiceEmail(
    email: string,
    invoiceId: string,
    orderNumber: string,
  ): Promise<void> {
    const response = await fetch(`${this.baseUrl}/v3/mail/send`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        personalizations: [
          {
            to: [{ email }],
            custom_args: { invoiceId, orderNumber },
          },
        ],
        from: { email: 'noreply@eda.local' },
        subject: `Invoice ${invoiceId} for order ${orderNumber}`,
        content: [
          {
            type: 'text/plain',
            value: `Your invoice ${invoiceId} is ready.`,
          },
        ],
      }),
    });

    if (!response.ok) {
      throw new Error(`SendGrid mock returned HTTP ${response.status}`);
    }

    this.logger.log(`Invoice email accepted by SendGrid for ${email}`);
  }
}
```

### Step 10.3 — Analytics command publisher port

- [x] Create `services/notification/src/messaging/analytics-command.publisher.ts`:

```typescript
import { EmailDelivered, EmailFailed } from '@eda/contracts';

export const ANALYTICS_COMMAND_PUBLISHER = Symbol('ANALYTICS_COMMAND_PUBLISHER');

export interface AnalyticsCommandPublisher {
  publishEmailDelivered(payload: EmailDelivered): Promise<void>;
  publishEmailFailed(payload: EmailFailed): Promise<void>;
}
```

- [x] Create `services/notification/src/messaging/rabbitmq-analytics-command.publisher.ts`:

```typescript
import {
  EmailDelivered,
  EmailDeliveredSchema,
  EmailFailed,
  EmailFailedSchema,
  ROUTING_KEYS,
} from '@eda/contracts';
import { Inject, Injectable } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { AnalyticsCommandPublisher } from './analytics-command.publisher';

@Injectable()
export class RabbitMqAnalyticsCommandPublisher
  implements AnalyticsCommandPublisher
{
  constructor(
    @Inject('RABBITMQ_COMMANDS') private readonly client: ClientProxy,
  ) {}

  async publishEmailDelivered(payload: EmailDelivered): Promise<void> {
    const command = EmailDeliveredSchema.parse(payload);
    await firstValueFrom(
      this.client.emit(ROUTING_KEYS.EMAIL_DELIVERED, command),
    );
  }

  async publishEmailFailed(payload: EmailFailed): Promise<void> {
    const command = EmailFailedSchema.parse(payload);
    await firstValueFrom(
      this.client.emit(ROUTING_KEYS.EMAIL_FAILED, command),
    );
  }
}
```

### Step 10.4 — Notification service

- [x] Replace `services/notification/src/notification/notification.service.ts`:

```typescript
import {
  EmailDeliveredSchema,
  EmailFailedSchema,
  InvoiceCreated,
  SendGridWebhookEvent,
  SendGridWebhookEventSchema,
} from '@eda/contracts';
import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  ANALYTICS_COMMAND_PUBLISHER,
  AnalyticsCommandPublisher,
} from '../messaging/analytics-command.publisher';
import { EMAIL_GATEWAY, EmailGateway } from '../gateways/email.gateway';

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);

  constructor(
    @Inject(EMAIL_GATEWAY) private readonly emailGateway: EmailGateway,
    @Inject(ANALYTICS_COMMAND_PUBLISHER)
    private readonly analyticsCommands: AnalyticsCommandPublisher,
  ) {}

  async sendInvoiceNotification(event: InvoiceCreated): Promise<void> {
    await this.emailGateway.sendInvoiceEmail(
      event.customerInfo.email,
      event.invoiceId,
      event.orderNumber,
    );
  }

  async handleSendGridEvent(raw: SendGridWebhookEvent): Promise<void> {
    const event = SendGridWebhookEventSchema.parse(raw);

    if (event.event === 'processed') {
      this.logger.log(`SendGrid processed: ${event.email}`);
      return;
    }

    if (!event.invoiceId || !event.orderNumber) {
      throw new Error('SendGrid webhook missing invoiceId or orderNumber');
    }

    if (event.event === 'delivered') {
      await this.analyticsCommands.publishEmailDelivered(
        EmailDeliveredSchema.parse({
          invoiceId: event.invoiceId,
          orderNumber: event.orderNumber,
          email: event.email,
          sgMessageId: event.sg_message_id,
        }),
      );
      return;
    }

    if (event.event === 'bounce' || event.event === 'dropped') {
      await this.analyticsCommands.publishEmailFailed(
        EmailFailedSchema.parse({
          invoiceId: event.invoiceId,
          orderNumber: event.orderNumber,
          email: event.email,
          reason: event.reason ?? event.event,
        }),
      );
    }
  }
}
```

### Step 10.5 — SendGrid webhook controller (thin)

- [x] Replace `services/notification/src/webhooks/webhooks.controller.ts`:

```typescript
import { SendGridWebhookPayloadSchema } from '@eda/contracts';
import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { NotificationService } from '../notification/notification.service';

@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly notificationService: NotificationService) {}

  @Post('sendgrid')
  @HttpCode(204)
  async sendgrid(@Body() body: unknown) {
    const events = SendGridWebhookPayloadSchema.parse(body);

    for (const event of events) {
      await this.notificationService.handleSendGridEvent(event);
    }
  }
}
```

### Step 10.6 — Kafka handler (thin)

- [x] Replace `services/notification/src/invoice-events/invoice-events.controller.ts` → rename to `invoice-events.handler.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InvoiceCreatedSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '../common/idempotency.store';
import { NotificationService } from '../notification/notification.service';

@Controller()
export class InvoiceEventsHandler {
  private readonly logger = new Logger(InvoiceEventsHandler.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly notificationService: NotificationService,
  ) {}

  @EventPattern(TOPICS.INVOICE_CREATED)
  async handleInvoiceCreated(@Payload() payload: unknown) {
    const event = InvoiceCreatedSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.invoiceId)) {
      this.logger.warn(`Duplicate billing.invoice.created: ${event.invoiceId}`);
      return;
    }

    await this.notificationService.sendInvoiceNotification(event);
    this.idempotency.markProcessed(event.invoiceId);
  }
}
```

### Step 10.7 — Wire modules and hybrid main

- [x] Replace `services/notification/src/notification/notification.module.ts`:

```typescript
import { EXCHANGES } from '@eda/contracts';
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { requireEnv } from '../common/env';
import { IdempotencyStore } from '../common/idempotency.store';
import { SendGridEmailGateway } from '../gateways/sendgrid-email.gateway';
import { EMAIL_GATEWAY } from '../gateways/email.gateway';
import { InvoiceEventsHandler } from '../invoice-events/invoice-events.handler';
import { ANALYTICS_COMMAND_PUBLISHER } from '../messaging/analytics-command.publisher';
import { RabbitMqAnalyticsCommandPublisher } from '../messaging/rabbitmq-analytics-command.publisher';
import { WebhooksController } from '../webhooks/webhooks.controller';
import { NotificationService } from './notification.service';

const rabbitmqRegister = ClientsModule.register([
  {
    name: 'RABBITMQ_COMMANDS',
    transport: Transport.RMQ,
    options: {
      urls: [requireEnv('RABBITMQ_URL')],
      queue: 'notifications.email.delivered',
      noAssert: true,
      queueOptions: { durable: true },
      wildcards: true,
      exchange: EXCHANGES.COMMANDS,
      exchangeTypes: 'topic',
    },
  },
]);

@Module({
  imports: [rabbitmqRegister],
  controllers: [InvoiceEventsHandler, WebhooksController],
  providers: [
    IdempotencyStore,
    NotificationService,
    {
      provide: EMAIL_GATEWAY,
      useClass: SendGridEmailGateway,
    },
    {
      provide: ANALYTICS_COMMAND_PUBLISHER,
      useClass: RabbitMqAnalyticsCommandPublisher,
    },
  ],
})
export class NotificationModule {}
```

- [x] Replace `services/notification/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './common/health.controller';
import { NotificationModule } from './notification/notification.module';

@Module({
  imports: [NotificationModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [x] Create `services/notification/src/main.ts`:

```typescript
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from './common/env';
import { ZodValidationExceptionFilter } from './common/zod-validation.filter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalFilters(new ZodValidationExceptionFilter());

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'notification-service',
        brokers: requireEnv('KAFKA_BROKERS').split(','),
      },
      consumer: { groupId: 'notification-service' },
      subscribe: { fromBeginning: false },
    },
  });

  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3050);
  await app.listen(port, '0.0.0.0');
  console.log(`notification-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 10.8 — Build all microservices

- [ ] Build each standalone service from its own folder:

```bash
for svc in api-gateway payment availability analytics invoice notification; do
  (cd "services/$svc" && pnpm build)
done
```

**Expected:** All six services build without errors.

### Checkpoint

- All 6 microservices build with `pnpm build` from their own folders.
- Mocks (Phases 3–4) still build via root `npx nest build <mock>`.
- SendGrid mock posts Event Webhook; notification emits RabbitMQ commands; analytics records email events.

### Suggested commit

```bash
git add services/notification
git commit -m "feat: add SendGrid webhook handler and analytics command publisher"
```

---

## Phase 11 — Wire everything in Docker Compose

**Goal:** Containerize all services and run the full stack with one command.

### Step 11.1 — Dockerfile pattern

**Microservices** (Phases 5–10) are standalone apps — each Dockerfile builds `packages/contracts` first, then the service. **Mocks** (Phases 3–4) still use the root workspace + `nest-cli.json`.

Build context for all images is the **repository root** (same as `docker-compose.yml`).

- [ ] Create `services/api-gateway/Dockerfile`:

```dockerfile
# docker build -f services/api-gateway/Dockerfile .

FROM node:20-alpine AS builder
WORKDIR /app

COPY packages/contracts ./packages/contracts
RUN cd packages/contracts && corepack enable && pnpm install && pnpm build

COPY services/api-gateway ./services/api-gateway
WORKDIR /app/services/api-gateway
RUN corepack enable && pnpm install && pnpm build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/packages/contracts ./packages/contracts
COPY --from=builder /app/services/api-gateway/dist ./dist
COPY --from=builder /app/services/api-gateway/node_modules ./node_modules
COPY --from=builder /app/services/api-gateway/package.json ./package.json

EXPOSE 3000
CMD ["node", "dist/main.js"]
```

- [ ] Create equivalent Dockerfiles for the other microservices — copy `services/api-gateway/Dockerfile` and change only the folder name and `EXPOSE` port:

| Dockerfile path | Service folder | EXPOSE |
|-----------------|----------------|--------|
| `services/payment/Dockerfile` | `services/payment` | 3010 |
| `services/availability/Dockerfile` | `services/availability` | 3020 |
| `services/analytics/Dockerfile` | `services/analytics` | 3030 |
| `services/invoice/Dockerfile` | `services/invoice` | 3040 |
| `services/notification/Dockerfile` | `services/notification` | 3050 |

Example for `services/payment/Dockerfile` — replace every `api-gateway` path with `payment` and `EXPOSE 3010`.

#### Mocks — workspace Dockerfile (unchanged from Phases 3–4)

- [ ] Create `mocks/stripe-mock/Dockerfile`:

```dockerfile
# docker build -f mocks/stripe-mock/Dockerfile .

FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY nest-cli.json ./
COPY tsconfig.base.json ./
COPY packages/shared ./packages/shared
COPY mocks/stripe-mock ./mocks/stripe-mock

RUN corepack enable
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @eda/shared build
RUN npx nest build stripe-mock

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages ./packages
COPY --from=builder /app/mocks/stripe-mock/dist ./mocks/stripe-mock/dist
COPY --from=builder /app/mocks/stripe-mock/package.json ./mocks/stripe-mock/package.json

WORKDIR /app/mocks/stripe-mock
EXPOSE 3001
CMD ["node", "dist/main.js"]
```

- [ ] Create `mocks/sendgrid-mock/Dockerfile` — same pattern as stripe-mock, swap folder name and `EXPOSE 3002`.

### Step 11.2 — Extend `.env.example`

- [ ] Append these lines to `.env.example`:

```bash
# Application services
API_GATEWAY_PORT=3000
STRIPE_MOCK_URL=http://stripe-mock:3001
SENDGRID_MOCK_URL=http://sendgrid-mock:3002
PAYMENT_WEBHOOK_URL=http://payment:3010/webhooks/stripe
NOTIFICATION_WEBHOOK_URL=http://notification:3050/webhooks/sendgrid

# Messaging URLs (Docker network — used inside containers)
KAFKA_BROKERS=kafka:9092
RABBITMQ_URL=amqp://eda_app:change-me-app-password@rabbitmq:5672/eda
```

- [ ] Copy to `.env` if not already done:

```bash
cp .env.example .env
```

### Step 11.3 — Extend `docker-compose.yml`

- [ ] Append these services to `docker-compose.yml` (after `kafka-init`):

```yaml
  stripe-mock:
    build:
      context: .
      dockerfile: mocks/stripe-mock/Dockerfile
    container_name: eda-stripe-mock
    restart: unless-stopped
    environment:
      PORT: 3001
      PAYMENT_WEBHOOK_URL: ${PAYMENT_WEBHOOK_URL:-http://payment:3010/webhooks/stripe}
    networks:
      - eda-network

  sendgrid-mock:
    build:
      context: .
      dockerfile: mocks/sendgrid-mock/Dockerfile
    container_name: eda-sendgrid-mock
    restart: unless-stopped
    environment:
      PORT: 3002
      NOTIFICATION_WEBHOOK_URL: ${NOTIFICATION_WEBHOOK_URL:-http://notification:3050/webhooks/sendgrid}
    networks:
      - eda-network

  payment:
    build:
      context: .
      dockerfile: services/payment/Dockerfile
    container_name: eda-payment
    restart: unless-stopped
    depends_on:
      rabbitmq-init:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
      stripe-mock:
        condition: service_started
    environment:
      PORT: 3010
      RABBITMQ_URL: ${RABBITMQ_URL:-amqp://eda_app:change-me-app-password@rabbitmq:5672/eda}
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
      STRIPE_MOCK_URL: ${STRIPE_MOCK_URL:-http://stripe-mock:3001}
    networks:
      - eda-network

  api-gateway:
    build:
      context: .
      dockerfile: services/api-gateway/Dockerfile
    container_name: eda-api-gateway
    restart: unless-stopped
    depends_on:
      rabbitmq-init:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
      payment:
        condition: service_started
    environment:
      PORT: ${API_GATEWAY_PORT:-3000}
      RABBITMQ_URL: ${RABBITMQ_URL:-amqp://eda_app:change-me-app-password@rabbitmq:5672/eda}
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
    networks:
      - eda-network

  availability:
    build:
      context: .
      dockerfile: services/availability/Dockerfile
    container_name: eda-availability
    restart: unless-stopped
    depends_on:
      kafka-init:
        condition: service_completed_successfully
    environment:
      PORT: 3020
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
    networks:
      - eda-network

  analytics:
    build:
      context: .
      dockerfile: services/analytics/Dockerfile
    container_name: eda-analytics
    restart: unless-stopped
    depends_on:
      rabbitmq-init:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
    environment:
      PORT: 3030
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
      RABBITMQ_URL: ${RABBITMQ_URL:-amqp://eda_app:change-me-app-password@rabbitmq:5672/eda}
    networks:
      - eda-network

  invoice:
    build:
      context: .
      dockerfile: services/invoice/Dockerfile
    container_name: eda-invoice
    restart: unless-stopped
    depends_on:
      kafka-init:
        condition: service_completed_successfully
    environment:
      PORT: 3040
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
    networks:
      - eda-network

  notification:
    build:
      context: .
      dockerfile: services/notification/Dockerfile
    container_name: eda-notification
    restart: unless-stopped
    depends_on:
      rabbitmq-init:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
      sendgrid-mock:
        condition: service_started
    environment:
      PORT: 3050
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
      RABBITMQ_URL: ${RABBITMQ_URL:-amqp://eda_app:change-me-app-password@rabbitmq:5672/eda}
      SENDGRID_MOCK_URL: ${SENDGRID_MOCK_URL:-http://sendgrid-mock:3002}
    networks:
      - eda-network
```

### Step 11.4 — Extend `docker-compose.dev.yml`

- [ ] Append port mappings for local access:

```yaml
  api-gateway:
    ports:
      - "127.0.0.1:3000:3000"

  analytics:
    ports:
      - "127.0.0.1:3030:3030"
```

### Step 11.5 — Build and start the full stack

- [ ] Run:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

**Expected:** All containers start. Application logs show `listening on` for each service.

- [ ] Verify health:

```bash
curl -s http://localhost:3000/health
curl -s http://localhost:3030/health
```

**Expected:** Both return `{"status":"ok"}`.

### Checkpoint

- Full stack runs in Docker.
- `api-gateway` and `analytics` respond on localhost.

### Suggested commit

```bash
git add docker-compose.yml docker-compose.dev.yml .env.example services/*/Dockerfile mocks/*/Dockerfile
git commit -m "feat: wire all services into Docker Compose"
```

---

## Phase 12 — End-to-end manual test

**Goal:** Trigger the full payment flow and verify every step.

### Step 12.1 — Open SSE stream (terminal 1)

- [ ] Start listening for events (replace `ORDER_NUMBER` after step 12.2, or use a placeholder and restart):

```bash
curl -N http://localhost:3000/orders/ORDER_NUMBER/events
```

> **Tip:** Run step 12.2 first to get an `orderNumber`, then run this command with the real UUID.

### Step 12.2 — Create an order (terminal 2)

- [ ] Run:

```bash
curl -s -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -d '{
    "productId": "prod_123",
    "customerEmail": "customer@example.com",
    "amount": 99.99
  }'
```

**Expected:**

```json
{"orderNumber":"<uuid>","status":"payment_pending"}
```

- [ ] Copy the `orderNumber` value.

### Step 12.2.1 — Verify Zod rejects invalid input (optional)

- [ ] Send a bad payload:

```bash
curl -s -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -d '{"productId":"","customerEmail":"not-an-email","amount":-1}'
```

**Expected:** HTTP `400` with `{ "statusCode": 400, "message": "Validation failed", "issues": [...] }`.

### Step 12.3 — Stream SSE for that order (terminal 1)

- [ ] Run with your actual order number:

```bash
curl -N http://localhost:3000/orders/<orderNumber>/events
```

**Expected** (within ~2 seconds):

```text
data: {"status":"payment_succeeded","orderNumber":"<uuid>"}
```

### Step 12.4 — Verify order status

- [ ] Run:

```bash
curl -s http://localhost:3000/orders/<orderNumber>
```

**Expected:** `"status":"payment_succeeded"`

### Step 12.5 — Verify analytics recorded all events

- [ ] Run:

```bash
curl -s http://localhost:3030/events | python3 -m json.tool
```

**Expected:** Array with at least four entries:
- `{ "type": "orders.payment.succeeded", ... }`
- `{ "type": "billing.invoice.created", ... }`
- `{ "type": "notifications.email.delivered", ... }`

### Step 12.5b — Verify email bounce path (optional)

- [ ] Create order with customer email containing `bounce@` (e.g. `bounce@test.local`).

- [ ] Wait for full flow (~25s for Stripe webhook + ~2s for SendGrid webhook).

- [ ] Run:

```bash
curl -s http://localhost:3030/events | python3 -m json.tool
```

**Expected:** Entry `{ "type": "notifications.email.failed", ... }` with `reason` field.

### Step 12.6 — Verify broker state

- [ ] **RabbitMQ UI** (http://localhost:15672): queues `orders.payment.requested`, `notifications.email.delivered`, and `notifications.email.failed` should have processed messages (Ready ≈ 0 after consumption).
- [ ] **Kafka UI** (http://localhost:8080): topics `orders.payment.succeeded` and `billing.invoice.created` show new messages.
- [ ] **Container logs:**

```bash
docker logs eda-payment --tail 20
docker logs eda-invoice --tail 20
docker logs eda-notification --tail 20
docker logs eda-sendgrid-mock --tail 20
```

**Expected:** Payment processed, invoice created, SendGrid accepted email, Event Webhook delivered, analytics recorded email command.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Missing required environment variable` | `.env` not loaded or incomplete | Copy `.env.example` → `.env`, restart compose |
| `ECONNREFUSED` to RabbitMQ/Kafka | Wrong URL for context | Inside Docker use `rabbitmq:5672` / `kafka:9092`; on host use `localhost:5672` / `localhost:9094` |
| SSE never fires | Kafka consumer not connected | Check `docker logs eda-api-gateway`; verify `KAFKA_BROKERS` |
| Message in DLQ | Payment service threw error | Check `docker logs eda-payment`; fix and requeue manually in RabbitMQ UI |
| `Unknown topic` error | Kafka init failed | Run `docker logs eda-kafka-init`; recreate with `docker compose ... up -d` |
| Webhook not received | stripe-mock cannot reach payment | Verify `PAYMENT_WEBHOOK_URL=http://payment:3010/webhooks/stripe` on same Docker network |
| SendGrid webhook not received | sendgrid-mock cannot reach notification | Verify `NOTIFICATION_WEBHOOK_URL=http://notification:3050/webhooks/sendgrid` on same Docker network |
| Email event missing in analytics | RabbitMQ command not consumed | Check `docker logs eda-notification` and `docker logs eda-analytics`; verify email queues in RabbitMQ UI |
| Duplicate processing | Idempotency working correctly | Expected on redelivery; check logs for `Duplicate` warnings |

### Checkpoint

- Full flow works: HTTP → RabbitMQ → Payment → Kafka → Invoice → Notification → SendGrid → Webhook → RabbitMQ → Analytics.

### Suggested commit

No code changes — optionally document your test order number in personal notes.

---

## Phase 13 — Production notes

**Read-only.** Do not implement these in the tutorial — use as a checklist when moving beyond local dev.

### Infrastructure

- [ ] **Kafka:** 3+ brokers, `KAFKA_REPLICATION_FACTOR=3`, `min.insync.replicas=2`
- [ ] **RabbitMQ:** Cluster with quorum queues across nodes
- [ ] **TLS:** Enable AMQPS and Kafka SASL/SSL — no plaintext in production
- [ ] **Secrets:** Vault, AWS Secrets Manager, or K8s secrets — not `.env` files on disk

### Application

- [ ] **Input validation:** Keep Zod at HTTP, webhook, and messaging boundaries; evolve schemas in `@eda/contracts` with versioned compatibility rules
- [ ] **Idempotency:** Replace in-memory `IdempotencyStore` with Redis or PostgreSQL
- [ ] **Schema Registry:** Confluent Schema Registry or Apicurio for Avro/Protobuf evolution
- [ ] **Outbox pattern:** Transactional outbox for reliable publish-after-db-write
- [ ] **Dead letters:** Monitor `orders.payment.requested.dlq`; alert and replay tooling
- [ ] **SSE at scale:** Replace in-memory `OrderStatusStreamService` with Redis Pub/Sub

### Operations

- [ ] **Observability:** OpenTelemetry traces across HTTP → RabbitMQ → Kafka
- [ ] **Metrics:** RabbitMQ Prometheus plugin (already enabled), Kafka JMX, service RED metrics
- [ ] **CI/CD:** Path-filtered builds — only rebuild changed service folders (`services/payment/`, etc.)
- [ ] **Deploy:** Kubernetes + Helm, or one Docker image per standalone service

### Repository strategy

- [ ] **Multi-project repo** (current, Phases 5–10): one Git clone, independent Nest apps — common in small teams and learning setups
- [ ] **Monorepo with shared libs** (Phases 0–4 mocks + `packages/contracts`): useful when many services share code and CI
- [ ] **Polyrepo:** split when teams need independent release cycles; publish `@eda/contracts` to a private registry or use Schema Registry

---

## Quick reference

### Service ports (local dev)

| Service | Port | Health check |
|---------|------|--------------|
| api-gateway | 3000 | http://localhost:3000/health |
| stripe-mock | 3001 | internal |
| sendgrid-mock | 3002 | internal |
| payment | 3010 | internal |
| availability | 3020 | internal |
| analytics | 3030 | http://localhost:3030/health |
| invoice | 3040 | internal |
| notification | 3050 | internal |
| RabbitMQ UI | 15672 | http://localhost:15672 |
| Kafka UI | 8080 | http://localhost:8080 |

### Useful commands

```bash
# Start everything (Docker)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

# Run a microservice locally (from its folder)
cd services/api-gateway && pnpm start:dev

# Lint / format a microservice (from its folder)
cd services/api-gateway && pnpm lint
cd services/api-gateway && pnpm lint:fix

# Lint entire repo (from root — includes mocks, packages, services)
pnpm lint

# Build all microservices
for svc in api-gateway payment availability analytics invoice notification; do
  (cd "services/$svc" && pnpm build)
done

# Build mocks (from repo root)
npx nest build stripe-mock
npx nest build sendgrid-mock

# Stop everything
docker compose -f docker-compose.yml -f docker-compose.dev.yml down

# Stop and wipe broker data
docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v

# Rebuild a single service
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build api-gateway

# Follow logs
docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f api-gateway payment
```

### Host vs Docker networking

| Running from | RabbitMQ | Kafka |
|--------------|----------|-------|
| Inside Docker container | `amqp://eda_app:PASS@rabbitmq:5672/eda` | `kafka:9092` |
| On your Mac (host) | `amqp://eda_app:PASS@localhost:5672/eda` | `localhost:9094` |

---

**Congratulations.** When all phases are checked off, you have a working event-driven architecture with an API gateway, six standalone NestJS microservices, mock external providers (workspace-based), shared event contracts, and production-oriented messaging infrastructure.

