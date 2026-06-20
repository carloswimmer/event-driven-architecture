# Event-Driven Architecture — Manual Implementation Tutorial

> **Goal:** Build the full payment/order flow from the architecture diagram using NestJS + Fastify, `@nestjs/microservices`, `nest-cli`, RabbitMQ (point-to-point), and Kafka (pub/sub) — entirely by hand, one checkbox at a time.

**Stack:** Node.js 20+, TypeScript, NestJS with Fastify adapter, `@nestjs/microservices`, nest-cli monorepo, pnpm workspaces, mock Stripe/SendGrid.

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
```

### GCP diagram → local stack

| GCP (original diagram) | Your stack | Pattern |
|------------------------|------------|---------|
| Cloud Task | **RabbitMQ** | Point-to-point: one worker per message |
| Pub/Sub | **Kafka** | Fan-out: many consumers per topic |
| Stripe | `mocks/stripe-mock` | HTTP + webhook callback |
| SendGrid | `mocks/sendgrid-mock` | HTTP email API |

### Event payloads

| Event | Transport | Payload |
|-------|-----------|---------|
| `orders.payment.requested` | RabbitMQ routing key | `{ reserveId, orderNumber, amount, customerEmail }` |
| `orders.payment.succeeded` | Kafka topic | `{ reserveId, value, customerInfo, orderNumber }` |
| `billing.invoice.created` | Kafka topic | `{ value, customerInfo, orderNumber, invoiceId }` |

### Naming convention (namespaced)

Commands and events use a **`<domain>.<entity>.<action>`** pattern so multiple teams can share the same brokers without collisions:

| Kind | Pattern | Examples |
|------|---------|----------|
| Command (RabbitMQ) | `<domain>.<entity>.<verb>` | `orders.payment.requested` |
| Event (Kafka) | `<domain>.<entity>.<past-tense>` | `orders.payment.succeeded`, `billing.invoice.created` |
| Dead letter | `<command>.dlq` / `<command>.failed` | `orders.payment.requested.dlq` |

Define names once in `@eda/contracts` (`ROUTING_KEYS`, `TOPICS`) — never hardcode strings in services.

### Final repo layout

```text
event-driven-architecture/
├── .cursor/tutorial.md
├── package.json
├── nest-cli.json          # monorepo registry — projects added incrementally
├── tsconfig.base.json
├── docker-compose.yml
├── docker-compose.dev.yml
├── .env.example
├── packages/
│   ├── contracts/
│   └── shared/
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

- [x] Create `packages/contracts/src/index.ts`:

```typescript
export * from './routing-keys';
export * from './topics';
export * from './events/payment-requested';
export * from './events/payment-succeeded';
export * from './events/invoice-created';
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

**Goal:** Install microservice transport dependencies, create an empty Nest monorepo skeleton, and learn the `@nestjs/microservices` patterns used in Phases 3–10. **Projects are registered in `nest-cli.json` incrementally** — only when you implement each app (Phase 3 adds `stripe-mock`, Phase 5 adds `api-gateway`, etc.).

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

### Step 1.5.4 — How to register a project (repeat in every app phase)

When you start a new app (mock or microservice), add an entry to `nest-cli.json` → `projects`:

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

**Example** — when you reach Phase 5 (`api-gateway`):

```json
"api-gateway": {
  "type": "application",
  "root": "services/api-gateway",
  "entryFile": "main",
  "sourceRoot": "services/api-gateway/src",
  "compilerOptions": {
    "tsConfigPath": "services/api-gateway/tsconfig.json"
  }
}
```

If it is the **first** project registered in the monorepo, also update the top-level defaults:

```json
"root": "services/api-gateway",
"sourceRoot": "services/api-gateway/src"
```

Individual per-app `nest-cli.json` files are **not** needed.

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

All commands use `npx nest` and run from the **repository root** (where `nest-cli.json` lives):

```bash
# Build / run a registered project (after you add it to nest-cli.json)
npx nest build api-gateway
npx nest start payment --watch
npx nest start api-gateway --debug --watch

# Scaffold inside a project (after src/main.ts exists)
npx nest g module orders --project api-gateway --no-spec
npx nest g controller orders --project api-gateway --no-spec
npx nest g service orders --project api-gateway --no-spec
npx nest g resource events --project analytics --no-spec
```

When running from inside a service directory (via `pnpm --filter`), pass the config path:

```bash
npx nest build api-gateway --config ../../nest-cli.json
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

| Convention | Detail |
|------------|--------|
| Monorepo CLI | Root `nest-cli.json` — register each app when you build it; run `npx nest build <project>` from repo root |
| Dev server | `npx nest start <project> --watch` from repo root |
| HTTP adapter | `NestFactory.create(AppModule, new FastifyAdapter())` |
| Hybrid apps | `connectMicroservice()` before `startAllMicroservices()` + `listen()` |
| Commands (RabbitMQ) | `@EventPattern(ROUTING_KEYS.…)` consumer; `client.emit()` producer |
| Events (Kafka) | `@EventPattern(TOPICS.…)` consumer; `ClientKafkaProxy.emit()` producer |
| Transport package | `@nestjs/microservices` (`Transport.RMQ`, `Transport.KAFKA`) |
| Health | `GET /health` via `HealthController` |
| Env vars | `requireEnv('VAR_NAME')` at startup |
| Idempotency key | `orderNumber` for payment flow; `invoiceId` for invoice flow |
| Kafka message key | Always `orderNumber` in production; Nest `emit()` sends value only (partition routing is a production refinement) |
| Consumer groups | One group per service: `api-gateway`, `payment-service`, etc. |

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

- [x] Register `stripe-mock` in root `nest-cli.json` → `projects` (see [Step 1.5.4](#step-154--how-to-register-a-project-repeat-in-every-app-phase)):

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

**Goal:** Simulate SendGrid email delivery API.

**Port:** `3002` | **Package:** `@eda/sendgrid-mock` | **Nest project:** `sendgrid-mock`

### Step 4.1 — Register project and scaffold

- [ ] Register `sendgrid-mock` in root `nest-cli.json` → `projects`:

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

- [ ] Create `mocks/sendgrid-mock/package.json`:

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

- [ ] Create `mocks/sendgrid-mock/tsconfig.json`:

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

- [ ] Create `mocks/sendgrid-mock/src/mail.controller.ts`:

```typescript
import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { MailService } from './mail.service';

@Controller('v3/mail')
export class MailController {
  constructor(private readonly mailService: MailService) {}

  @Post('send')
  @HttpCode(202)
  send(@Body() body: { personalizations: Array<{ to: Array<{ email: string }> }>; subject: string }) {
    return this.mailService.send(body);
  }
}
```

- [ ] Create `mocks/sendgrid-mock/src/mail.service.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class MailService {
  private readonly logger = new Logger(MailService.name);

  send(body: {
    personalizations: Array<{ to: Array<{ email: string }> }>;
    subject: string;
  }) {
    const recipient = body.personalizations?.[0]?.to?.[0]?.email ?? 'unknown';
    this.logger.log(`Email sent to ${recipient}: ${body.subject}`);
    return { message: 'accepted' };
  }
}
```

- [ ] Create `mocks/sendgrid-mock/src/app.module.ts`:

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

- [ ] Create `mocks/sendgrid-mock/src/main.ts`:

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

### Suggested commit

```bash
git add nest-cli.json mocks/sendgrid-mock
git commit -m "feat: add mock SendGrid email service"
```

---

## Phase 5 — API Gateway / Order Service

**Goal:** HTTP entry point — reserve product, publish `orders.payment.requested`, stream SSE when payment succeeds. Uses a **hybrid app** (HTTP + Kafka consumer) and `ClientsModule` for RabbitMQ publishing.

**Port:** `3000` | **Package:** `@eda/api-gateway` | **Nest project:** `api-gateway`

### Step 5.1 — Register project and scaffold with nest-cli

- [ ] Register `api-gateway` in root `nest-cli.json` → `projects`:

```json
"api-gateway": {
  "type": "application",
  "root": "services/api-gateway",
  "entryFile": "main",
  "sourceRoot": "services/api-gateway/src",
  "compilerOptions": {
    "tsConfigPath": "services/api-gateway/tsconfig.json"
  }
}
```

- [ ] Create the service directory and `package.json`:

```json
{
  "name": "@eda/api-gateway",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build api-gateway --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start api-gateway --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "amqp-connection-manager": "^4.0.0",
    "amqplib": "^0.10.5",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/api-gateway/tsconfig.json`:

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

- [ ] Create minimal bootstrap files so `npx nest g` can scaffold into the project:

```bash
mkdir -p services/api-gateway/src
```

- [ ] Create `services/api-gateway/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';

@Module({})
export class AppModule {}
```

- [ ] Create `services/api-gateway/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  await NestFactory.create(AppModule);
}
bootstrap();
```

- [ ] Scaffold modules with `npx nest` (from **repository root**):

```bash
npx nest g module orders --project api-gateway --no-spec
npx nest g service orders --project api-gateway --no-spec
npx nest g controller orders --project api-gateway --no-spec
npx nest g controller payment-events --project api-gateway --no-spec
```

**Expected:** Nest creates `orders.module.ts`, `orders.service.ts`, `orders.controller.ts`, and `payment-events.controller.ts` under `services/api-gateway/src/`.

> **Note:** No per-service `nest-cli.json` — only the root registry, updated in this step.

### Step 5.2 — Order store

- [ ] Create `services/api-gateway/src/orders.store.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { Subject } from 'rxjs';

export type OrderStatus = 'payment_pending' | 'payment_succeeded';

export interface Order {
  orderNumber: string;
  reserveId: string;
  productId: string;
  amount: number;
  customerEmail: string;
  status: OrderStatus;
}

@Injectable()
export class OrdersStore {
  private readonly orders = new Map<string, Order>();
  private readonly events = new Map<string, Subject<Order>>();

  create(order: Order): void {
    this.orders.set(order.orderNumber, order);
    this.events.set(order.orderNumber, new Subject<Order>());
  }

  get(orderNumber: string): Order | undefined {
    return this.orders.get(orderNumber);
  }

  markPaid(orderNumber: string): void {
    const order = this.orders.get(orderNumber);
    if (!order) return;
    order.status = 'payment_succeeded';
    this.events.get(orderNumber)?.next(order);
    this.events.get(orderNumber)?.complete();
  }

  subscribe(orderNumber: string): Subject<Order> | undefined {
    return this.events.get(orderNumber);
  }
}
```

### Step 5.3 — RabbitMQ publisher (`ClientsModule` + `emit`)

- [ ] Replace `services/api-gateway/src/orders.service.ts`:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { PaymentRequested, ROUTING_KEYS } from '@eda/contracts';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class OrdersService {
  constructor(
    @Inject('RABBITMQ_COMMANDS') private readonly rabbitClient: ClientProxy,
  ) {}

  async publishPaymentRequested(payload: PaymentRequested): Promise<void> {
    await firstValueFrom(
      this.rabbitClient.emit(ROUTING_KEYS.PAYMENT_REQUESTED, payload),
    );
  }
}
```

### Step 5.4 — Kafka consumer (`@EventPattern`)

- [ ] Replace `services/api-gateway/src/payment-events.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { OrdersStore } from './orders.store';

@Controller()
export class PaymentEventsController {
  private readonly logger = new Logger(PaymentEventsController.name);

  constructor(private readonly ordersStore: OrdersStore) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);
    this.logger.log(`Payment succeeded for order ${event.orderNumber}`);
    this.ordersStore.markPaid(event.orderNumber);
  }
}
```

### Step 5.5 — HTTP controller

- [ ] Replace `services/api-gateway/src/orders.controller.ts`:

```typescript
import {
  Body,
  Controller,
  Get,
  NotFoundException,
  Param,
  Post,
  Sse,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Observable, map } from 'rxjs';
import { OrdersStore } from './orders.store';
import { OrdersService } from './orders.service';

@Controller('orders')
export class OrdersController {
  constructor(
    private readonly ordersStore: OrdersStore,
    private readonly ordersService: OrdersService,
  ) {}

  @Post()
  async createOrder(
    @Body() body: { productId: string; customerEmail: string; amount: number },
  ) {
    const orderNumber = randomUUID();
    const reserveId = randomUUID();

    this.ordersStore.create({
      orderNumber,
      reserveId,
      productId: body.productId,
      amount: body.amount,
      customerEmail: body.customerEmail,
      status: 'payment_pending',
    });

    await this.ordersService.publishPaymentRequested({
      reserveId,
      orderNumber,
      amount: body.amount,
      customerEmail: body.customerEmail,
    });

    return { orderNumber, status: 'payment_pending' };
  }

  @Sse(':orderNumber/events')
  stream(@Param('orderNumber') orderNumber: string) {
    const subject = this.ordersStore.subscribe(orderNumber);
    if (!subject) {
      throw new NotFoundException(`Order ${orderNumber} not found`);
    }
    return subject.pipe(
      map((order) => ({ data: { status: order.status, orderNumber: order.orderNumber } })),
    );
  }

  @Get(':orderNumber')
  getOrder(@Param('orderNumber') orderNumber: string) {
    const order = this.ordersStore.get(orderNumber);
    if (!order) {
      throw new NotFoundException(`Order ${orderNumber} not found`);
    }
    return order;
  }
}
```

### Step 5.6 — Wire modules (hybrid app)

- [ ] Replace `services/api-gateway/src/orders.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { EXCHANGES } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';
import { OrdersStore } from './orders.store';
import { PaymentEventsController } from './payment-events.controller';

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
  controllers: [OrdersController, PaymentEventsController],
  providers: [OrdersStore, OrdersService],
})
export class OrdersModule {}
```

- [ ] Replace `services/api-gateway/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { OrdersModule } from './orders.module';

@Module({
  imports: [OrdersModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Replace `services/api-gateway/src/main.ts` (hybrid HTTP + Kafka):

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { requireEnv } from '@eda/shared';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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

### Step 5.7 — Build

- [ ] From the **repository root**:

```bash
pnpm install
npx nest build api-gateway
```

**Expected:** Build succeeds.

### Checkpoint

- `@eda/api-gateway` builds via `npx nest build api-gateway`.
- Flow: `POST /orders` → `client.emit()` to RabbitMQ; `@EventPattern` Kafka handler → SSE push.

### Suggested commit

```bash
git add nest-cli.json services/api-gateway
git commit -m "feat: add api-gateway with Nest microservices hybrid app"
```

---

## Phase 6 — Payment Service

**Goal:** Consume `orders.payment.requested` from RabbitMQ, call Stripe mock, publish `orders.payment.succeeded` to Kafka on webhook. Uses a **hybrid app** (RabbitMQ consumer + HTTP webhook).

**Port:** `3010` | **Package:** `@eda/payment` | **Nest project:** `payment`

### Step 6.1 — Register project and scaffold with nest-cli

- [ ] Register `payment` in root `nest-cli.json` → `projects`:

```json
"payment": {
  "type": "application",
  "root": "services/payment",
  "entryFile": "main",
  "sourceRoot": "services/payment/src",
  "compilerOptions": {
    "tsConfigPath": "services/payment/tsconfig.json"
  }
}
```

- [ ] Create `services/payment/package.json`:

```json
{
  "name": "@eda/payment",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build payment --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start payment --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "amqp-connection-manager": "^4.0.0",
    "amqplib": "^0.10.5",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/payment/tsconfig.json`:

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

- [ ] Bootstrap and scaffold (from **repository root**):

```bash
mkdir -p services/payment/src
```

Create minimal `services/payment/src/app.module.ts` and `main.ts` (same stub pattern as Phase 5), then:

```bash
npx nest g module payment --project payment --no-spec
npx nest g controller payment-consumer --project payment --no-spec
npx nest g controller webhooks --project payment --no-spec
npx nest g service kafka-producer --project payment --no-spec
```

### Step 6.2 — Kafka producer (`ClientKafkaProxy`)

- [ ] Replace `services/payment/src/kafka-producer.service.ts`:

```typescript
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka, ClientKafkaProxy } from '@nestjs/microservices';
import { PaymentSucceeded, TOPICS } from '@eda/contracts';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafka: ClientKafkaProxy,
  ) {}

  async onModuleInit() {
    await (this.kafka as ClientKafka).connect();
  }

  async publishPaymentSucceeded(event: PaymentSucceeded): Promise<void> {
    await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_SUCCEEDED, event));
  }

  async onModuleDestroy() {
    await (this.kafka as ClientKafka).close();
  }
}
```

### Step 6.3 — RabbitMQ consumer (`@EventPattern` + manual ack)

- [ ] Replace `services/payment/src/payment-consumer.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';
import { PaymentRequestedSchema, ROUTING_KEYS } from '@eda/contracts';
import { IdempotencyStore, requireEnv } from '@eda/shared';

@Controller()
export class PaymentConsumerController {
  private readonly logger = new Logger(PaymentConsumerController.name);

  constructor(private readonly idempotency: IdempotencyStore) {}

  @EventPattern(ROUTING_KEYS.PAYMENT_REQUESTED)
  async handlePaymentRequested(
    @Payload() payload: unknown,
    @Ctx() context: RmqContext,
  ) {
    const channel = context.getChannelRef();
    const msg = context.getMessage();
    const stripeMockUrl = requireEnv('STRIPE_MOCK_URL');

    try {
      const data = PaymentRequestedSchema.parse(payload);

      if (this.idempotency.isDuplicate(data.orderNumber)) {
        this.logger.warn(`Duplicate orders.payment.requested: ${data.orderNumber}`);
        channel.ack(msg);
        return;
      }

      this.logger.log(`Processing payment for order ${data.orderNumber}`);

      const response = await fetch(`${stripeMockUrl}/v1/payment-intents`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          orderNumber: data.orderNumber,
          amount: data.amount,
          reserveId: data.reserveId,
          customerEmail: data.customerEmail,
        }),
      });

      if (!response.ok) {
        throw new Error(`Stripe mock returned HTTP ${response.status}`);
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

### Step 6.4 — Stripe webhook controller

- [ ] Replace `services/payment/src/webhooks.controller.ts`:

```typescript
import { Body, Controller, Post } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';

@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly kafka: KafkaProducerService) {}

  @Post('stripe')
  async stripeWebhook(
    @Body()
    body: {
      type: string;
      data: {
        orderNumber: string;
        amount: number;
        reserveId: string;
        customerEmail: string;
      };
    },
  ) {
    if (body.type !== 'payment_intent.succeeded') {
      return { received: true };
    }

    await this.kafka.publishPaymentSucceeded({
      reserveId: body.data.reserveId,
      value: body.data.amount,
      customerInfo: { email: body.data.customerEmail },
      orderNumber: body.data.orderNumber,
    });

    return { received: true };
  }
}
```

### Step 6.5 — Wire modules (hybrid app)

- [ ] Replace `services/payment/src/payment.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import { PaymentConsumerController } from './payment-consumer.controller';
import { WebhooksController } from './webhooks.controller';
import { KafkaProducerService } from './kafka-producer.service';

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
  controllers: [PaymentConsumerController, WebhooksController],
  providers: [IdempotencyStore, KafkaProducerService],
})
export class PaymentModule {}
```

- [ ] Replace `services/payment/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { PaymentModule } from './payment.module';

@Module({
  imports: [PaymentModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Replace `services/payment/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { EXCHANGES } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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
  console.log(`payment-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 6.6 — Build

- [ ] From the **repository root**:

```bash
npx nest build payment
```

**Expected:** Build succeeds.

### Checkpoint

- Payment service builds via `npx nest build payment`.
- Flow: `@EventPattern` RabbitMQ consume → Stripe mock → webhook → `kafka.emit()`.

### Suggested commit

```bash
git add nest-cli.json services/payment
git commit -m "feat: add payment service with Nest hybrid RMQ consumer and Kafka producer"
```

---

## Phase 7 — Availability Service

**Goal:** Consume `orders.payment.succeeded` and decrement mock inventory. Hybrid app (HTTP health + Kafka consumer).

**Port:** `3020` | **Package:** `@eda/availability` | **Nest project:** `availability`

### Step 7.1 — Register project and scaffold with nest-cli

- [ ] Register `availability` in root `nest-cli.json` → `projects`:

```json
"availability": {
  "type": "application",
  "root": "services/availability",
  "entryFile": "main",
  "sourceRoot": "services/availability/src",
  "compilerOptions": {
    "tsConfigPath": "services/availability/tsconfig.json"
  }
}
```

- [ ] Create `services/availability/package.json`:

```json
{
  "name": "@eda/availability",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build availability --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start availability --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/availability/tsconfig.json`:

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

- [ ] Bootstrap and scaffold (from **repository root**):

```bash
mkdir -p services/availability/src
```

Create minimal `app.module.ts` and `main.ts`, then:

```bash
npx nest g module availability --project availability --no-spec
npx nest g controller payment-events --project availability --no-spec
npx nest g service inventory --project availability --no-spec
```

### Step 7.2 — Inventory store

- [ ] Replace `services/availability/src/inventory.service.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class InventoryService {
  private readonly logger = new Logger(InventoryService.name);
  private stock = 100;

  confirmReservation(reserveId: string): void {
    this.stock -= 1;
    this.logger.log(
      `Confirmed reservation ${reserveId}. Remaining stock: ${this.stock}`,
    );
  }
}
```

### Step 7.3 — Kafka consumer (`@EventPattern`)

- [ ] Replace `services/availability/src/payment-events.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '@eda/shared';
import { InventoryService } from './inventory.service';

@Controller()
export class PaymentEventsController {
  private readonly logger = new Logger(PaymentEventsController.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly inventory: InventoryService,
  ) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.orderNumber)) {
      this.logger.warn(`Duplicate orders.payment.succeeded: ${event.orderNumber}`);
      return;
    }

    this.inventory.confirmReservation(event.reserveId);
    this.idempotency.markProcessed(event.orderNumber);
  }
}
```

### Step 7.4 — Wire modules and hybrid main

- [ ] Replace `services/availability/src/availability.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { IdempotencyStore } from '@eda/shared';
import { InventoryService } from './inventory.service';
import { PaymentEventsController } from './payment-events.controller';

@Module({
  controllers: [PaymentEventsController],
  providers: [IdempotencyStore, InventoryService],
})
export class AvailabilityModule {}
```

- [ ] Create `services/availability/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { AvailabilityModule } from './availability.module';

@Module({
  imports: [AvailabilityModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Create `services/availability/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from '@eda/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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

### Step 7.5 — Build

- [ ] From the **repository root**:

```bash
npx nest build availability
```

**Expected:** Build succeeds.

### Checkpoint

- Availability service builds and subscribes to `orders.payment.succeeded` via `@EventPattern`.

### Suggested commit

```bash
git add nest-cli.json services/availability
git commit -m "feat: add availability service with Nest Kafka consumer"
```

---

## Phase 8 — Analytics Service

**Goal:** Consume both `orders.payment.succeeded` and `billing.invoice.created`; expose events for debugging. Hybrid app with two `@EventPattern` handlers on one Kafka consumer group.

**Port:** `3030` | **Package:** `@eda/analytics` | **Nest project:** `analytics`

### Step 8.1 — Register project and scaffold with nest-cli

- [ ] Register `analytics` in root `nest-cli.json` → `projects`:

```json
"analytics": {
  "type": "application",
  "root": "services/analytics",
  "entryFile": "main",
  "sourceRoot": "services/analytics/src",
  "compilerOptions": {
    "tsConfigPath": "services/analytics/tsconfig.json"
  }
}
```

- [ ] Create `services/analytics/package.json`:

```json
{
  "name": "@eda/analytics",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build analytics --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start analytics --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/analytics/tsconfig.json`:

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

- [ ] Bootstrap and scaffold (from **repository root**):

```bash
mkdir -p services/analytics/src
```

Create minimal `app.module.ts` and `main.ts`, then:

```bash
npx nest g module analytics --project analytics --no-spec
npx nest g controller events --project analytics --no-spec
npx nest g controller kafka-events --project analytics --no-spec
npx nest g service events --project analytics --no-spec
```

### Step 8.2 — Event store and HTTP controller

- [ ] Replace `services/analytics/src/events.service.ts`:

```typescript
import { Injectable } from '@nestjs/common';

@Injectable()
export class EventsService {
  private readonly events: Array<{ type: string; payload: unknown; at: string }> = [];

  append(type: string, payload: unknown): void {
    this.events.push({ type, payload, at: new Date().toISOString() });
  }

  list() {
    return this.events;
  }
}
```

- [ ] Replace `services/analytics/src/events.controller.ts`:

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

### Step 8.3 — Kafka event handlers (`@EventPattern`)

- [ ] Replace `services/analytics/src/kafka-events.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  InvoiceCreatedSchema,
  PaymentSucceededSchema,
  TOPICS,
} from '@eda/contracts';
import { EventsService } from './events.service';

@Controller()
export class KafkaEventsController {
  private readonly logger = new Logger(KafkaEventsController.name);

  constructor(private readonly eventsService: EventsService) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);
    this.logger.log(`Recorded orders.payment.succeeded: ${event.orderNumber}`);
    this.eventsService.append('orders.payment.succeeded', event);
  }

  @EventPattern(TOPICS.INVOICE_CREATED)
  handleInvoiceCreated(@Payload() payload: unknown) {
    const event = InvoiceCreatedSchema.parse(payload);
    this.logger.log(`Recorded billing.invoice.created: ${event.invoiceId}`);
    this.eventsService.append('billing.invoice.created', event);
  }
}
```

### Step 8.4 — Wire modules and hybrid main

- [ ] Replace `services/analytics/src/analytics.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { EventsController } from './events.controller';
import { EventsService } from './events.service';
import { KafkaEventsController } from './kafka-events.controller';

@Module({
  controllers: [EventsController, KafkaEventsController],
  providers: [EventsService],
})
export class AnalyticsModule {}
```

- [ ] Create `services/analytics/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { AnalyticsModule } from './analytics.module';

@Module({
  imports: [AnalyticsModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Create `services/analytics/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from '@eda/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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

### Step 8.5 — Build

- [ ] From the **repository root**:

```bash
npx nest build analytics
```

**Expected:** Build succeeds.

### Checkpoint

- Analytics service builds.
- `GET /events` returns recorded events after E2E test.

### Suggested commit

```bash
git add nest-cli.json services/analytics
git commit -m "feat: add analytics service with Nest Kafka event handlers"
```

---

## Phase 9 — Invoice Service

**Goal:** Consume `orders.payment.succeeded`, publish `billing.invoice.created`. Hybrid app with Kafka consumer + Kafka producer.

**Port:** `3040` | **Package:** `@eda/invoice` | **Nest project:** `invoice`

### Step 9.1 — Register project and scaffold with nest-cli

- [ ] Register `invoice` in root `nest-cli.json` → `projects`:

```json
"invoice": {
  "type": "application",
  "root": "services/invoice",
  "entryFile": "main",
  "sourceRoot": "services/invoice/src",
  "compilerOptions": {
    "tsConfigPath": "services/invoice/tsconfig.json"
  }
}
```

- [ ] Create `services/invoice/package.json`:

```json
{
  "name": "@eda/invoice",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build invoice --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start invoice --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/invoice/tsconfig.json`:

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

- [ ] Bootstrap and scaffold (from **repository root**):

```bash
mkdir -p services/invoice/src
```

Create minimal `app.module.ts` and `main.ts`, then:

```bash
npx nest g module invoice --project invoice --no-spec
npx nest g controller payment-events --project invoice --no-spec
npx nest g service kafka-producer --project invoice --no-spec
```

### Step 9.2 — Kafka producer

- [ ] Replace `services/invoice/src/kafka-producer.service.ts`:

```typescript
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka, ClientKafkaProxy } from '@nestjs/microservices';
import { InvoiceCreated, TOPICS } from '@eda/contracts';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafka: ClientKafkaProxy,
  ) {}

  async onModuleInit() {
    await (this.kafka as ClientKafka).connect();
  }

  async publishInvoiceCreated(event: InvoiceCreated): Promise<void> {
    await firstValueFrom(this.kafka.emit(TOPICS.INVOICE_CREATED, event));
  }

  async onModuleDestroy() {
    await (this.kafka as ClientKafka).close();
  }
}
```

### Step 9.3 — Kafka consumer (`@EventPattern`)

- [ ] Replace `services/invoice/src/payment-events.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '@eda/shared';
import { randomUUID } from 'crypto';
import { KafkaProducerService } from './kafka-producer.service';

@Controller()
export class PaymentEventsController {
  private readonly logger = new Logger(PaymentEventsController.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly producer: KafkaProducerService,
  ) {}

  @EventPattern(TOPICS.PAYMENT_SUCCEEDED)
  async handlePaymentSucceeded(@Payload() payload: unknown) {
    const event = PaymentSucceededSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.orderNumber)) {
      this.logger.warn(`Duplicate orders.payment.succeeded: ${event.orderNumber}`);
      return;
    }

    const invoiceId = randomUUID();
    this.logger.log(`Creating invoice ${invoiceId} for order ${event.orderNumber}`);

    await this.producer.publishInvoiceCreated({
      invoiceId,
      value: event.value,
      customerInfo: event.customerInfo,
      orderNumber: event.orderNumber,
    });

    this.idempotency.markProcessed(event.orderNumber);
  }
}
```

### Step 9.4 — Wire modules and hybrid main

- [ ] Replace `services/invoice/src/invoice.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import { PaymentEventsController } from './payment-events.controller';
import { KafkaProducerService } from './kafka-producer.service';

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
  controllers: [PaymentEventsController],
  providers: [IdempotencyStore, KafkaProducerService],
})
export class InvoiceModule {}
```

- [ ] Create `services/invoice/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { InvoiceModule } from './invoice.module';

@Module({
  imports: [InvoiceModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Create `services/invoice/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from '@eda/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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

### Step 9.5 — Build

- [ ] From the **repository root**:

```bash
npx nest build invoice
```

**Expected:** Build succeeds.

### Checkpoint

- Invoice service builds.
- Consumes `orders.payment.succeeded`, publishes `billing.invoice.created` via `emit()`.

### Suggested commit

```bash
git add nest-cli.json services/invoice
git commit -m "feat: add invoice service with Nest Kafka consume and publish"
```

---

## Phase 10 — Notification Service

**Goal:** Consume `billing.invoice.created`, send email via SendGrid mock. Hybrid app (HTTP health + Kafka consumer).

**Port:** `3050` | **Package:** `@eda/notification` | **Nest project:** `notification`

### Step 10.1 — Register project and scaffold with nest-cli

- [ ] Register `notification` in root `nest-cli.json` → `projects`:

```json
"notification": {
  "type": "application",
  "root": "services/notification",
  "entryFile": "main",
  "sourceRoot": "services/notification/src",
  "compilerOptions": {
    "tsConfigPath": "services/notification/tsconfig.json"
  }
}
```

- [ ] Create `services/notification/package.json`:

```json
{
  "name": "@eda/notification",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "npx nest build notification --config ../../nest-cli.json",
    "start": "node dist/main.js",
    "start:dev": "npx nest start notification --watch --config ../../nest-cli.json"
  },
  "dependencies": {
    "@eda/contracts": "workspace:*",
    "@eda/shared": "workspace:*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/microservices": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "kafkajs": "^2.2.4",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `services/notification/tsconfig.json`:

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

- [ ] Bootstrap and scaffold (from **repository root**):

```bash
mkdir -p services/notification/src
```

Create minimal `app.module.ts` and `main.ts`, then:

```bash
npx nest g module notification --project notification --no-spec
npx nest g controller invoice-events --project notification --no-spec
npx nest g service sendgrid --project notification --no-spec
```

### Step 10.2 — SendGrid client

- [ ] Replace `services/notification/src/sendgrid.service.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { requireEnv } from '@eda/shared';

@Injectable()
export class SendGridService {
  private readonly logger = new Logger(SendGridService.name);
  private readonly baseUrl = requireEnv('SENDGRID_MOCK_URL');

  async sendInvoiceEmail(email: string, invoiceId: string, orderNumber: string) {
    const response = await fetch(`${this.baseUrl}/v3/mail/send`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        personalizations: [{ to: [{ email }] }],
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

    this.logger.log(`Invoice email sent to ${email}`);
  }
}
```

### Step 10.3 — Kafka consumer (`@EventPattern`)

- [ ] Replace `services/notification/src/invoice-events.controller.ts`:

```typescript
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InvoiceCreatedSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore } from '@eda/shared';
import { SendGridService } from './sendgrid.service';

@Controller()
export class InvoiceEventsController {
  private readonly logger = new Logger(InvoiceEventsController.name);

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly sendGrid: SendGridService,
  ) {}

  @EventPattern(TOPICS.INVOICE_CREATED)
  async handleInvoiceCreated(@Payload() payload: unknown) {
    const event = InvoiceCreatedSchema.parse(payload);

    if (this.idempotency.isDuplicate(event.invoiceId)) {
      this.logger.warn(`Duplicate billing.invoice.created: ${event.invoiceId}`);
      return;
    }

    await this.sendGrid.sendInvoiceEmail(
      event.customerInfo.email,
      event.invoiceId,
      event.orderNumber,
    );

    this.idempotency.markProcessed(event.invoiceId);
  }
}
```

### Step 10.4 — Wire modules and hybrid main

- [ ] Replace `services/notification/src/notification.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { IdempotencyStore } from '@eda/shared';
import { InvoiceEventsController } from './invoice-events.controller';
import { SendGridService } from './sendgrid.service';

@Module({
  controllers: [InvoiceEventsController],
  providers: [IdempotencyStore, SendGridService],
})
export class NotificationModule {}
```

- [ ] Create `services/notification/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { NotificationModule } from './notification.module';

@Module({
  imports: [NotificationModule],
  controllers: [HealthController],
})
export class AppModule {}
```

- [ ] Create `services/notification/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { requireEnv } from '@eda/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

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

### Step 10.5 — Build all services

- [ ] From the **repository root**:

```bash
npx nest build api-gateway
npx nest build payment
npx nest build availability
npx nest build analytics
npx nest build invoice
npx nest build notification
```

Or:

```bash
pnpm run build:all
```

**Expected:** All workspace packages build without errors.

### Checkpoint

- All 6 services + 2 mocks build successfully via `npx nest build <project>`.

### Suggested commit

```bash
git add nest-cli.json services/notification
git commit -m "feat: add notification service with Nest Kafka consumer"
```

---

## Phase 11 — Wire everything in Docker Compose

**Goal:** Containerize all services and run the full stack with one command.

### Step 11.1 — Shared Dockerfile pattern

Each service and mock uses the same multi-stage build from the **repository root** as build context. Create this Dockerfile in each app directory.

- [ ] Create `services/api-gateway/Dockerfile` (copy the same file to every service and mock, changing only `SERVICE_PATH`):

```dockerfile
# Build from repository root:
# docker build -f services/api-gateway/Dockerfile .

FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY nest-cli.json ./
COPY tsconfig.base.json ./
COPY packages/contracts ./packages/contracts
COPY packages/shared ./packages/shared
COPY services/api-gateway ./services/api-gateway

RUN corepack enable
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @eda/contracts build
RUN pnpm --filter @eda/shared build
RUN npx nest build api-gateway

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages ./packages
COPY --from=builder /app/services/api-gateway/dist ./services/api-gateway/dist
COPY --from=builder /app/services/api-gateway/package.json ./services/api-gateway/package.json

WORKDIR /app/services/api-gateway
EXPOSE 3000
CMD ["node", "dist/main.js"]
```

- [ ] Create equivalent Dockerfiles by copying and adjusting paths:

| Dockerfile path | Nest build command | EXPOSE | WORKDIR |
|-----------------|-------------------|--------|---------|
| `services/payment/Dockerfile` | `npx nest build payment` | 3010 | `services/payment` |
| `services/availability/Dockerfile` | `npx nest build availability` | 3020 | `services/availability` |
| `services/analytics/Dockerfile` | `npx nest build analytics` | 3030 | `services/analytics` |
| `services/invoice/Dockerfile` | `npx nest build invoice` | 3040 | `services/invoice` |
| `services/notification/Dockerfile` | `npx nest build notification` | 3050 | `services/notification` |
| `mocks/stripe-mock/Dockerfile` | `npx nest build stripe-mock` | 3001 | `mocks/stripe-mock` |
| `mocks/sendgrid-mock/Dockerfile` | `npx nest build sendgrid-mock` | 3002 | `mocks/sendgrid-mock` |

Example for `services/payment/Dockerfile` — replace `api-gateway` with `payment`, `npx nest build payment`, EXPOSE `3010`.

Example for `mocks/stripe-mock/Dockerfile` — copy only `packages/shared` (not contracts unless needed), build `@eda/stripe-mock`.

#### Complete Dockerfile: `services/payment/Dockerfile`

```dockerfile
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY nest-cli.json ./
COPY tsconfig.base.json ./
COPY packages/contracts ./packages/contracts
COPY packages/shared ./packages/shared
COPY services/payment ./services/payment

RUN corepack enable
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @eda/contracts build
RUN pnpm --filter @eda/shared build
RUN npx nest build payment

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages ./packages
COPY --from=builder /app/services/payment/dist ./services/payment/dist
COPY --from=builder /app/services/payment/package.json ./services/payment/package.json

WORKDIR /app/services/payment
EXPOSE 3010
CMD ["node", "dist/main.js"]
```

#### Complete Dockerfile: `mocks/stripe-mock/Dockerfile`

```dockerfile
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

For the remaining Dockerfiles (`availability`, `analytics`, `invoice`, `notification`, `sendgrid-mock`), follow the same pattern: swap the service folder name and workspace build target.

### Step 11.2 — Extend `.env.example`

- [ ] Append these lines to `.env.example`:

```bash
# Application services
API_GATEWAY_PORT=3000
STRIPE_MOCK_URL=http://stripe-mock:3001
SENDGRID_MOCK_URL=http://sendgrid-mock:3002
PAYMENT_WEBHOOK_URL=http://payment:3010/webhooks/stripe

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
      kafka-init:
        condition: service_completed_successfully
    environment:
      PORT: 3030
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
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
      kafka-init:
        condition: service_completed_successfully
      sendgrid-mock:
        condition: service_started
    environment:
      PORT: 3050
      KAFKA_BROKERS: ${KAFKA_BROKERS:-kafka:9092}
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

### Step 12.5 — Verify analytics recorded both events

- [ ] Run:

```bash
curl -s http://localhost:3030/events | python3 -m json.tool
```

**Expected:** Array with at least two entries:
- `{ "type": "orders.payment.succeeded", ... }`
- `{ "type": "billing.invoice.created", ... }`

### Step 12.6 — Verify broker state

- [ ] **RabbitMQ UI** (http://localhost:15672): queue `orders.payment.requested` should have processed messages (Ready ≈ 0 after consumption).
- [ ] **Kafka UI** (http://localhost:8080): topics `orders.payment.succeeded` and `billing.invoice.created` show new messages.
- [ ] **Container logs:**

```bash
docker logs eda-payment --tail 20
docker logs eda-invoice --tail 20
docker logs eda-notification --tail 20
docker logs eda-sendgrid-mock --tail 20
```

**Expected:** Payment processed, invoice created, email sent log in sendgrid-mock.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Missing required environment variable` | `.env` not loaded or incomplete | Copy `.env.example` → `.env`, restart compose |
| `ECONNREFUSED` to RabbitMQ/Kafka | Wrong URL for context | Inside Docker use `rabbitmq:5672` / `kafka:9092`; on host use `localhost:5672` / `localhost:9094` |
| SSE never fires | Kafka consumer not connected | Check `docker logs eda-api-gateway`; verify `KAFKA_BROKERS` |
| Message in DLQ | Payment service threw error | Check `docker logs eda-payment`; fix and requeue manually in RabbitMQ UI |
| `Unknown topic` error | Kafka init failed | Run `docker logs eda-kafka-init`; recreate with `docker compose ... up -d` |
| Webhook not received | stripe-mock cannot reach payment | Verify `PAYMENT_WEBHOOK_URL=http://payment:3010/webhooks/stripe` on same Docker network |
| Duplicate processing | Idempotency working correctly | Expected on redelivery; check logs for `Duplicate` warnings |

### Checkpoint

- Full flow works: HTTP → RabbitMQ → Payment → Kafka → Invoice → Notification → SSE.

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

- [ ] **Idempotency:** Replace in-memory `IdempotencyStore` with Redis or PostgreSQL
- [ ] **Schema Registry:** Confluent Schema Registry or Apicurio for Avro/Protobuf evolution
- [ ] **Outbox pattern:** Transactional outbox for reliable publish-after-db-write
- [ ] **Dead letters:** Monitor `orders.payment.requested.dlq`; alert and replay tooling
- [ ] **SSE at scale:** Replace in-memory Subject with Redis Pub/Sub or dedicated push service

### Operations

- [ ] **Observability:** OpenTelemetry traces across HTTP → RabbitMQ → Kafka
- [ ] **Metrics:** RabbitMQ Prometheus plugin (already enabled), Kafka JMX, service RED metrics
- [ ] **CI/CD:** Path-filtered builds — only rebuild changed services in the monorepo
- [ ] **Deploy:** Kubernetes + Helm, or separate images per service from the monorepo

### Repository strategy

- [ ] **Monorepo** (current): great for small teams and shared contracts
- [ ] **Polyrepo:** split when teams need independent release cycles; extract `@eda/contracts` to its own repo or Schema Registry

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
# Start everything
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

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

**Congratulations.** When all phases are checked off, you have a working event-driven architecture with an API gateway, six NestJS microservices (using `@nestjs/microservices` + `nest-cli`), mock external providers, and production-oriented messaging infrastructure.

