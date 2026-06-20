# Event-Driven Architecture — Manual Implementation Tutorial

> **Goal:** Build the full payment/order flow from the architecture diagram using NestJS + Fastify, RabbitMQ (point-to-point), and Kafka (pub/sub) — entirely by hand, one checkbox at a time.

**Stack:** Node.js 20+, TypeScript, NestJS with Fastify adapter, npm workspaces monorepo, mock Stripe/SendGrid.

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
- **npm 10+** — verify: `npm -v` → `10.x.x` or higher
- **curl** — for HTTP/SSE testing
- Repository cloned at `event-driven-architecture/`

---

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
| `payment.requested` | RabbitMQ routing key | `{ reserveId, orderNumber, amount, customerEmail }` |
| `paymentSucceeded` | Kafka topic | `{ reserveId, value, customerInfo, orderNumber }` |
| `invoiceCreated` | Kafka topic | `{ value, customerInfo, orderNumber, invoiceId }` |

### Final repo layout

```text
event-driven-architecture/
├── .cursor/tutorial.md
├── package.json
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

- [ ] Copy the environment template:

```bash
cp .env.example .env
```

- [ ] Open `.env` and confirm these values exist (defaults are fine for local dev):

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

- [ ] Run:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

**Expected:** All long-running services reach `healthy` or `running`. Init containers `eda-rabbitmq-init` and `eda-kafka-init` exit with code `0`.

- [ ] Check status:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml ps
```

**Expected:** `eda-rabbitmq`, `eda-kafka`, `eda-kafka-ui` are `Up (healthy)`. Init containers show `Exited (0)`.

### Step 0.3 — Verify RabbitMQ topology

- [ ] Open RabbitMQ Management UI: http://localhost:15672
- [ ] Login: `admin` / `change-me-admin-password` (or your `.env` values)
- [ ] Confirm:
  - Vhost **`eda`** exists
  - Exchange **`eda.commands`** (direct, durable)
  - Queue **`payment.requested`** (quorum, with DLX → `eda.dlx`)
  - Queue **`payment.requested.dlq`**
  - Application user **`eda_app`** exists with permissions on vhost `eda`

Topology is defined in `infra/rabbitmq/definitions.json` and imported by `infra/rabbitmq/init.sh`.

### Step 0.4 — Verify Kafka topics

- [ ] Open Kafka UI: http://localhost:8080
- [ ] Confirm topics exist:
  - **`paymentSucceeded`** — 6 partitions
  - **`invoiceCreated`** — 3 partitions

Topics are created by `infra/kafka/init-topics.sh`. Auto-create is **disabled** in `docker-compose.yml` (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`).

### Step 0.5 — Verify from CLI (optional)

- [ ] List Kafka topics:

```bash
docker exec eda-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Expected:**

```text
invoiceCreated
paymentSucceeded
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

**Goal:** Create the npm workspaces root and the `@eda/contracts` shared package.

### Step 1.1 — Root `package.json`

- [ ] Create `package.json` at the repository root:

```json
{
  "name": "event-driven-architecture",
  "private": true,
  "workspaces": [
    "packages/*",
    "services/*",
    "mocks/*"
  ],
  "scripts": {
    "build": "npm run build -w @eda/contracts && npm run build -w @eda/shared",
    "build:all": "npm run build && npm run build -ws --if-present"
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

### Step 1.2 — Root TypeScript config

- [ ] Create `tsconfig.base.json`:

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

- [ ] Create `packages/contracts/package.json`:

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

- [ ] Create `packages/contracts/tsconfig.json`:

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

- [ ] Create `packages/contracts/src/routing-keys.ts`:

```typescript
export const ROUTING_KEYS = {
  PAYMENT_REQUESTED: 'payment.requested',
} as const;

export const EXCHANGES = {
  COMMANDS: 'eda.commands',
} as const;
```

- [ ] Create `packages/contracts/src/topics.ts`:

```typescript
export const TOPICS = {
  PAYMENT_SUCCEEDED: 'paymentSucceeded',
  INVOICE_CREATED: 'invoiceCreated',
} as const;
```

- [ ] Create `packages/contracts/src/events/payment-requested.ts`:

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

- [ ] Create `packages/contracts/src/events/payment-succeeded.ts`:

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

- [ ] Create `packages/contracts/src/events/invoice-created.ts`:

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

- [ ] Create `packages/contracts/src/index.ts`:

```typescript
export * from './routing-keys';
export * from './topics';
export * from './events/payment-requested';
export * from './events/payment-succeeded';
export * from './events/invoice-created';
```

### Step 1.4 — Install and build

- [ ] From the repository root:

```bash
npm install
npm run build -w @eda/contracts
```

**Expected:** `packages/contracts/dist/` is created with compiled `.js` and `.d.ts` files. No TypeScript errors.

### Checkpoint

- `npm run build -w @eda/contracts` succeeds.
- You can import `@eda/contracts` from other workspace packages.

### Suggested commit

```bash
git add package.json tsconfig.base.json packages/contracts package-lock.json
git commit -m "feat: add monorepo scaffolding and shared event contracts"
```

---

## Phase 2 — Shared NestJS patterns

**Goal:** Create `@eda/shared` with utilities every service reuses: health check, idempotency store, env helper.

### Step 2.1 — Shared package scaffold

- [ ] Create `packages/shared/package.json`:

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
    "@eda/contracts": "*",
    "@nestjs/common": "^10.4.15"
  }
}
```

- [ ] Create `packages/shared/tsconfig.json`:

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

- [ ] Create `packages/shared/src/idempotency.store.ts`:

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

- [ ] Create `packages/shared/src/env.ts`:

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

- [ ] Create `packages/shared/src/health.controller.ts`:

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

- [ ] Create `packages/shared/src/index.ts`:

```typescript
export * from './idempotency.store';
export * from './env';
export * from './health.controller';
```

### Step 2.6 — Build shared package

- [ ] Run:

```bash
npm install
npm run build -w @eda/shared
```

**Expected:** `packages/shared/dist/` created successfully.

### Conventions used in every service

| Convention | Detail |
|------------|--------|
| HTTP adapter | `NestFactory.create(AppModule, new FastifyAdapter())` |
| Health | `GET /health` via `HealthController` |
| Env vars | `requireEnv('VAR_NAME')` at startup |
| Idempotency key | `orderNumber` for payment flow; `invoiceId` for invoice flow |
| Kafka message key | Always `orderNumber` (matches partition routing in `infra/kafka/init-topics.sh`) |
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

**Port:** `3001` | **Package:** `@eda/stripe-mock`

### Step 3.1 — Package scaffold

- [ ] Create `mocks/stripe-mock/package.json`:

```json
{
  "name": "@eda/stripe-mock",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  }
}
```

- [ ] Create `mocks/stripe-mock/tsconfig.json`:

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

- [ ] Create `mocks/stripe-mock/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 3.2 — Stripe mock implementation

- [ ] Create `mocks/stripe-mock/src/stripe.controller.ts`:

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

- [ ] Create `mocks/stripe-mock/src/stripe.service.ts`:

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

- [ ] Create `mocks/stripe-mock/src/app.module.ts`:

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

- [ ] Create `mocks/stripe-mock/src/main.ts`:

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

- [ ] Run:

```bash
npm install
npm run build -w @eda/stripe-mock
PAYMENT_WEBHOOK_URL=http://localhost:3010/webhooks/stripe PORT=3001 npm run start -w @eda/stripe-mock
```

**Expected:** `stripe-mock listening on 3001` (Payment Service is not running yet — webhook will fail; that is OK for now).

Press `Ctrl+C` to stop.

### Checkpoint

- `@eda/stripe-mock` builds.
- `POST /v1/payment-intents` returns a mock intent ID.

### Suggested commit

```bash
git add mocks/stripe-mock
git commit -m "feat: add mock Stripe service with webhook simulation"
```

---

## Phase 4 — Mock SendGrid service

**Goal:** Simulate SendGrid email delivery API.

**Port:** `3002` | **Package:** `@eda/sendgrid-mock`

### Step 4.1 — Package scaffold

- [ ] Create `mocks/sendgrid-mock/package.json`:

```json
{
  "name": "@eda/sendgrid-mock",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/shared": "*",
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

- [ ] Create `mocks/sendgrid-mock/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
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
npm run build -w @eda/sendgrid-mock
```

**Expected:** Build succeeds with no errors.

### Checkpoint

- `@eda/sendgrid-mock` builds.
- Service accepts `POST /v3/mail/send` and returns HTTP 202.

### Suggested commit

```bash
git add mocks/sendgrid-mock
git commit -m "feat: add mock SendGrid email service"
```

---

## Phase 5 — API Gateway / Order Service

**Goal:** HTTP entry point — reserve product, publish `payment.requested`, stream SSE when payment succeeds.

**Port:** `3000` | **Package:** `@eda/api-gateway`

### Step 5.1 — Package scaffold

- [ ] Create `services/api-gateway/package.json`:

```json
{
  "name": "@eda/api-gateway",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
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

- [ ] Create `services/api-gateway/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 5.2 — Order store and SSE

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

### Step 5.3 — RabbitMQ publisher

- [ ] Create `services/api-gateway/src/rabbitmq.service.ts`:

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EXCHANGES, PaymentRequested, ROUTING_KEYS } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import * as amqp from 'amqplib';

@Injectable()
export class RabbitMqService implements OnModuleInit, OnModuleDestroy {
  private connection!: amqp.Connection;
  private channel!: amqp.Channel;

  async onModuleInit() {
    const url = requireEnv('RABBITMQ_URL');
    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();
  }

  async publishPaymentRequested(payload: PaymentRequested): Promise<void> {
    const body = Buffer.from(JSON.stringify(payload));
    const sent = this.channel.publish(
      EXCHANGES.COMMANDS,
      ROUTING_KEYS.PAYMENT_REQUESTED,
      body,
      { contentType: 'application/json', persistent: true },
    );
    if (!sent) {
      throw new Error('Failed to publish payment.requested — channel buffer full');
    }
  }

  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
  }
}
```

### Step 5.4 — Kafka consumer (paymentSucceeded)

- [ ] Create `services/api-gateway/src/kafka.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { Kafka, Consumer } from 'kafkajs';
import { OrdersStore } from './orders.store';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer!: Consumer;

  constructor(private readonly ordersStore: OrdersStore) {}

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'api-gateway', brokers });
    this.consumer = kafka.consumer({ groupId: 'api-gateway' });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCEEDED, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const parsed = PaymentSucceededSchema.parse(JSON.parse(raw));
        this.logger.log(`Payment succeeded for order ${parsed.orderNumber}`);
        this.ordersStore.markPaid(parsed.orderNumber);
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
  }
}
```

### Step 5.5 — HTTP controller

- [ ] Create `services/api-gateway/src/orders.controller.ts`:

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
import { RabbitMqService } from './rabbitmq.service';

@Controller('orders')
export class OrdersController {
  constructor(
    private readonly ordersStore: OrdersStore,
    private readonly rabbitMq: RabbitMqService,
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

    await this.rabbitMq.publishPaymentRequested({
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

### Step 5.6 — App module and main

- [ ] Create `services/api-gateway/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { OrdersController } from './orders.controller';
import { OrdersStore } from './orders.store';
import { RabbitMqService } from './rabbitmq.service';
import { KafkaConsumerService } from './kafka.consumer';

@Module({
  controllers: [HealthController, OrdersController],
  providers: [OrdersStore, RabbitMqService, KafkaConsumerService],
})
export class AppModule {}
```

- [ ] Create `services/api-gateway/src/main.ts`:

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

- [ ] Run:

```bash
npm install
npm run build -w @eda/api-gateway
```

**Expected:** Build succeeds.

### Checkpoint

- `@eda/api-gateway` builds.
- You understand the flow: `POST /orders` → RabbitMQ publish; Kafka consumer → SSE push.

### Suggested commit

```bash
git add services/api-gateway
git commit -m "feat: add api-gateway with RabbitMQ publish and Kafka SSE"
```

---

## Phase 6 — Payment Service

**Goal:** Consume `payment.requested` from RabbitMQ, call Stripe mock, publish `paymentSucceeded` to Kafka on webhook.

**Port:** `3010` | **Package:** `@eda/payment`

### Step 6.1 — Package scaffold

- [ ] Create `services/payment/package.json`:

```json
{
  "name": "@eda/payment",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
    "@nestjs/platform-fastify": "^10.4.15",
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

- [ ] Create `services/payment/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 6.2 — Kafka producer

- [ ] Create `services/payment/src/kafka.producer.ts`:

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PaymentSucceeded, TOPICS } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private producer!: Producer;

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'payment-service', brokers });
    this.producer = kafka.producer();
    await this.producer.connect();
  }

  async publishPaymentSucceeded(event: PaymentSucceeded): Promise<void> {
    await this.producer.send({
      topic: TOPICS.PAYMENT_SUCCEEDED,
      messages: [
        {
          key: event.orderNumber,
          value: JSON.stringify(event),
        },
      ],
    });
  }

  async onModuleDestroy() {
    await this.producer?.disconnect();
  }
}
```

### Step 6.3 — RabbitMQ consumer

- [ ] Create `services/payment/src/rabbitmq.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PaymentRequestedSchema } from '@eda/contracts';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import * as amqp from 'amqplib';

@Injectable()
export class RabbitMqConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqConsumerService.name);
  private connection!: amqp.Connection;
  private channel!: amqp.Channel;

  constructor(
    private readonly idempotency: IdempotencyStore,
  ) {}

  async onModuleInit() {
    const url = requireEnv('RABBITMQ_URL');
    const stripeMockUrl = requireEnv('STRIPE_MOCK_URL');

    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();
    await this.channel.prefetch(1);

    await this.channel.consume('payment.requested', async (msg) => {
      if (!msg) return;
      try {
        const payload = PaymentRequestedSchema.parse(
          JSON.parse(msg.content.toString()),
        );

        if (this.idempotency.isDuplicate(payload.orderNumber)) {
          this.logger.warn(`Duplicate payment.requested: ${payload.orderNumber}`);
          this.channel.ack(msg);
          return;
        }

        this.logger.log(`Processing payment for order ${payload.orderNumber}`);

        const response = await fetch(`${stripeMockUrl}/v1/payment-intents`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            orderNumber: payload.orderNumber,
            amount: payload.amount,
            reserveId: payload.reserveId,
            customerEmail: payload.customerEmail,
          }),
        });

        if (!response.ok) {
          throw new Error(`Stripe mock returned HTTP ${response.status}`);
        }

        this.idempotency.markProcessed(payload.orderNumber);
        this.channel.ack(msg);
      } catch (err) {
        this.logger.error('Failed to process payment.requested', err);
        this.channel.nack(msg, false, false);
      }
    });
  }

  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
  }
}
```

### Step 6.4 — Stripe webhook controller

- [ ] Create `services/payment/src/webhooks.controller.ts`:

```typescript
import { Body, Controller, Post } from '@nestjs/common';
import { KafkaProducerService } from './kafka.producer';

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

### Step 6.5 — App module and main

- [ ] Create `services/payment/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController, IdempotencyStore } from '@eda/shared';
import { KafkaProducerService } from './kafka.producer';
import { RabbitMqConsumerService } from './rabbitmq.consumer';
import { WebhooksController } from './webhooks.controller';

@Module({
  controllers: [HealthController, WebhooksController],
  providers: [
    IdempotencyStore,
    KafkaProducerService,
    RabbitMqConsumerService,
  ],
})
export class AppModule {}
```

- [ ] Create `services/payment/src/main.ts`:

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

- [ ] Run:

```bash
npm run build -w @eda/payment
```

**Expected:** Build succeeds.

### Checkpoint

- Payment service builds.
- Flow: RabbitMQ consume → Stripe mock → webhook → Kafka publish.

### Suggested commit

```bash
git add services/payment
git commit -m "feat: add payment service with RabbitMQ consumer and Kafka producer"
```

---

## Phase 7 — Availability Service

**Goal:** Consume `paymentSucceeded` and decrement mock inventory.

**Port:** `3020` | **Package:** `@eda/availability`

### Step 7.1 — Package scaffold

- [ ] Create `services/availability/package.json`:

```json
{
  "name": "@eda/availability",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
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

- [ ] Create `services/availability/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 7.2 — Inventory store and Kafka consumer

- [ ] Create `services/availability/src/inventory.store.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';

@Injectable()
export class InventoryStore {
  private readonly logger = new Logger(InventoryStore.name);
  private stock = 100;

  confirmReservation(reserveId: string): void {
    this.stock -= 1;
    this.logger.log(
      `Confirmed reservation ${reserveId}. Remaining stock: ${this.stock}`,
    );
  }
}
```

- [ ] Create `services/availability/src/kafka.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import { Kafka, Consumer } from 'kafkajs';
import { InventoryStore } from './inventory.store';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer!: Consumer;

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly inventory: InventoryStore,
  ) {}

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'availability-service', brokers });
    this.consumer = kafka.consumer({ groupId: 'availability-service' });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCEEDED, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const event = PaymentSucceededSchema.parse(JSON.parse(raw));

        if (this.idempotency.isDuplicate(event.orderNumber)) {
          this.logger.warn(`Duplicate paymentSucceeded: ${event.orderNumber}`);
          return;
        }

        this.inventory.confirmReservation(event.reserveId);
        this.idempotency.markProcessed(event.orderNumber);
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
  }
}
```

- [ ] Create `services/availability/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController, IdempotencyStore } from '@eda/shared';
import { InventoryStore } from './inventory.store';
import { KafkaConsumerService } from './kafka.consumer';

@Module({
  controllers: [HealthController],
  providers: [IdempotencyStore, InventoryStore, KafkaConsumerService],
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
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3020);
  await app.listen(port, '0.0.0.0');
  console.log(`availability-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 7.3 — Build

- [ ] Run:

```bash
npm run build -w @eda/availability
```

**Expected:** Build succeeds.

### Checkpoint

- Availability service builds and subscribes to `paymentSucceeded`.

### Suggested commit

```bash
git add services/availability
git commit -m "feat: add availability service consuming paymentSucceeded"
```

---

## Phase 8 — Analytics Service

**Goal:** Consume both `paymentSucceeded` and `invoiceCreated`; expose events for debugging.

**Port:** `3030` | **Package:** `@eda/analytics`

### Step 8.1 — Package scaffold

- [ ] Create `services/analytics/package.json`:

```json
{
  "name": "@eda/analytics",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
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

- [ ] Create `services/analytics/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 8.2 — Event store and consumers

- [ ] Create `services/analytics/src/events.store.ts`:

```typescript
import { Injectable } from '@nestjs/common';

@Injectable()
export class EventsStore {
  private readonly events: Array<{ type: string; payload: unknown; at: string }> = [];

  append(type: string, payload: unknown): void {
    this.events.push({ type, payload, at: new Date().toISOString() });
  }

  list() {
    return this.events;
  }
}
```

- [ ] Create `services/analytics/src/events.controller.ts`:

```typescript
import { Controller, Get } from '@nestjs/common';
import { EventsStore } from './events.store';

@Controller('events')
export class EventsController {
  constructor(private readonly eventsStore: EventsStore) {}

  @Get()
  list() {
    return this.eventsStore.list();
  }
}
```

- [ ] Create `services/analytics/src/kafka.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  InvoiceCreatedSchema,
  PaymentSucceededSchema,
  TOPICS,
} from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { Kafka, Consumer } from 'kafkajs';
import { EventsStore } from './events.store';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private paymentConsumer!: Consumer;
  private invoiceConsumer!: Consumer;

  constructor(private readonly eventsStore: EventsStore) {}

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');

    const kafkaPayment = new Kafka({ clientId: 'analytics-payment', brokers });
    this.paymentConsumer = kafkaPayment.consumer({ groupId: 'analytics-service' });
    await this.paymentConsumer.connect();
    await this.paymentConsumer.subscribe({
      topic: TOPICS.PAYMENT_SUCCEEDED,
      fromBeginning: false,
    });

    await this.paymentConsumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const event = PaymentSucceededSchema.parse(JSON.parse(raw));
        this.logger.log(`Recorded paymentSucceeded: ${event.orderNumber}`);
        this.eventsStore.append('paymentSucceeded', event);
      },
    });

    const kafkaInvoice = new Kafka({ clientId: 'analytics-invoice', brokers });
    this.invoiceConsumer = kafkaInvoice.consumer({ groupId: 'analytics-service' });
    await this.invoiceConsumer.connect();
    await this.invoiceConsumer.subscribe({
      topic: TOPICS.INVOICE_CREATED,
      fromBeginning: false,
    });

    await this.invoiceConsumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const event = InvoiceCreatedSchema.parse(JSON.parse(raw));
        this.logger.log(`Recorded invoiceCreated: ${event.invoiceId}`);
        this.eventsStore.append('invoiceCreated', event);
      },
    });
  }

  async onModuleDestroy() {
    await this.paymentConsumer?.disconnect();
    await this.invoiceConsumer?.disconnect();
  }
}
```

- [ ] Create `services/analytics/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from '@eda/shared';
import { EventsController } from './events.controller';
import { EventsStore } from './events.store';
import { KafkaConsumerService } from './kafka.consumer';

@Module({
  controllers: [HealthController, EventsController],
  providers: [EventsStore, KafkaConsumerService],
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
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3030);
  await app.listen(port, '0.0.0.0');
  console.log(`analytics-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 8.3 — Build

- [ ] Run:

```bash
npm run build -w @eda/analytics
```

**Expected:** Build succeeds.

### Checkpoint

- Analytics service builds.
- `GET /events` will return recorded events after E2E test.

### Suggested commit

```bash
git add services/analytics
git commit -m "feat: add analytics service for payment and invoice events"
```

---

## Phase 9 — Invoice Service

**Goal:** Consume `paymentSucceeded`, publish `invoiceCreated`.

**Port:** `3040` | **Package:** `@eda/invoice`

### Step 9.1 — Package scaffold

- [ ] Create `services/invoice/package.json`:

```json
{
  "name": "@eda/invoice",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
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

- [ ] Create `services/invoice/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 9.2 — Kafka producer and consumer

- [ ] Create `services/invoice/src/kafka.producer.ts`:

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InvoiceCreated, TOPICS } from '@eda/contracts';
import { requireEnv } from '@eda/shared';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private producer!: Producer;

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'invoice-service', brokers });
    this.producer = kafka.producer();
    await this.producer.connect();
  }

  async publishInvoiceCreated(event: InvoiceCreated): Promise<void> {
    await this.producer.send({
      topic: TOPICS.INVOICE_CREATED,
      messages: [
        {
          key: event.orderNumber,
          value: JSON.stringify(event),
        },
      ],
    });
  }

  async onModuleDestroy() {
    await this.producer?.disconnect();
  }
}
```

- [ ] Create `services/invoice/src/kafka.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PaymentSucceededSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import { Kafka, Consumer } from 'kafkajs';
import { randomUUID } from 'crypto';
import { KafkaProducerService } from './kafka.producer';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer!: Consumer;

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly producer: KafkaProducerService,
  ) {}

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'invoice-service', brokers });
    this.consumer = kafka.consumer({ groupId: 'invoice-service' });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCEEDED, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const event = PaymentSucceededSchema.parse(JSON.parse(raw));

        if (this.idempotency.isDuplicate(event.orderNumber)) {
          this.logger.warn(`Duplicate paymentSucceeded: ${event.orderNumber}`);
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
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
  }
}
```

- [ ] Create `services/invoice/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController, IdempotencyStore } from '@eda/shared';
import { KafkaConsumerService } from './kafka.consumer';
import { KafkaProducerService } from './kafka.producer';

@Module({
  controllers: [HealthController],
  providers: [IdempotencyStore, KafkaProducerService, KafkaConsumerService],
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
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3040);
  await app.listen(port, '0.0.0.0');
  console.log(`invoice-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 9.3 — Build

- [ ] Run:

```bash
npm run build -w @eda/invoice
```

**Expected:** Build succeeds.

### Checkpoint

- Invoice service builds.
- Consumes `paymentSucceeded`, publishes `invoiceCreated`.

### Suggested commit

```bash
git add services/invoice
git commit -m "feat: add invoice service with Kafka consume and publish"
```

---

## Phase 10 — Notification Service

**Goal:** Consume `invoiceCreated`, send email via SendGrid mock.

**Port:** `3050` | **Package:** `@eda/notification`

### Step 10.1 — Package scaffold

- [ ] Create `services/notification/package.json`:

```json
{
  "name": "@eda/notification",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch"
  },
  "dependencies": {
    "@eda/contracts": "*",
    "@eda/shared": "*",
    "@nestjs/common": "^10.4.15",
    "@nestjs/core": "^10.4.15",
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

- [ ] Create `services/notification/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src",
  "compilerOptions": {
    "deleteOutDir": true
  }
}
```

### Step 10.2 — SendGrid client and Kafka consumer

- [ ] Create `services/notification/src/sendgrid.client.ts`:

```typescript
import { Injectable, Logger } from '@nestjs/common';
import { requireEnv } from '@eda/shared';

@Injectable()
export class SendGridClient {
  private readonly logger = new Logger(SendGridClient.name);
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

- [ ] Create `services/notification/src/kafka.consumer.ts`:

```typescript
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InvoiceCreatedSchema, TOPICS } from '@eda/contracts';
import { IdempotencyStore, requireEnv } from '@eda/shared';
import { Kafka, Consumer } from 'kafkajs';
import { SendGridClient } from './sendgrid.client';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer!: Consumer;

  constructor(
    private readonly idempotency: IdempotencyStore,
    private readonly sendGrid: SendGridClient,
  ) {}

  async onModuleInit() {
    const brokers = requireEnv('KAFKA_BROKERS').split(',');
    const kafka = new Kafka({ clientId: 'notification-service', brokers });
    this.consumer = kafka.consumer({ groupId: 'notification-service' });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: TOPICS.INVOICE_CREATED, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const raw = message.value?.toString();
        if (!raw) return;
        const event = InvoiceCreatedSchema.parse(JSON.parse(raw));

        if (this.idempotency.isDuplicate(event.invoiceId)) {
          this.logger.warn(`Duplicate invoiceCreated: ${event.invoiceId}`);
          return;
        }

        await this.sendGrid.sendInvoiceEmail(
          event.customerInfo.email,
          event.invoiceId,
          event.orderNumber,
        );

        this.idempotency.markProcessed(event.invoiceId);
      },
    });
  }

  async onModuleDestroy() {
    await this.consumer?.disconnect();
  }
}
```

- [ ] Create `services/notification/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { HealthController, IdempotencyStore } from '@eda/shared';
import { KafkaConsumerService } from './kafka.consumer';
import { SendGridClient } from './sendgrid.client';

@Module({
  controllers: [HealthController],
  providers: [IdempotencyStore, SendGridClient, KafkaConsumerService],
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
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );
  const port = Number(process.env.PORT ?? 3050);
  await app.listen(port, '0.0.0.0');
  console.log(`notification-service listening on ${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Step 10.3 — Build all services

- [ ] Run:

```bash
npm run build:all
```

**Expected:** All workspace packages build without errors.

### Checkpoint

- All 6 services + 2 mocks build successfully.

### Suggested commit

```bash
git add services/notification
git commit -m "feat: add notification service consuming invoiceCreated"
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

COPY package.json package-lock.json ./
COPY tsconfig.base.json ./
COPY packages/contracts ./packages/contracts
COPY packages/shared ./packages/shared
COPY services/api-gateway ./services/api-gateway

RUN npm ci
RUN npm run build -w @eda/contracts
RUN npm run build -w @eda/shared
RUN npm run build -w @eda/api-gateway

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

| Dockerfile path | Workspace build command | EXPOSE | WORKDIR |
|-----------------|------------------------|--------|---------|
| `services/payment/Dockerfile` | `@eda/payment` | 3010 | `services/payment` |
| `services/availability/Dockerfile` | `@eda/availability` | 3020 | `services/availability` |
| `services/analytics/Dockerfile` | `@eda/analytics` | 3030 | `services/analytics` |
| `services/invoice/Dockerfile` | `@eda/invoice` | 3040 | `services/invoice` |
| `services/notification/Dockerfile` | `@eda/notification` | 3050 | `services/notification` |
| `mocks/stripe-mock/Dockerfile` | `@eda/stripe-mock` | 3001 | `mocks/stripe-mock` |
| `mocks/sendgrid-mock/Dockerfile` | `@eda/sendgrid-mock` | 3002 | `mocks/sendgrid-mock` |

Example for `services/payment/Dockerfile` — replace `api-gateway` with `payment` and `@eda/api-gateway` with `@eda/payment`, EXPOSE `3010`.

Example for `mocks/stripe-mock/Dockerfile` — copy only `packages/shared` (not contracts unless needed), build `@eda/stripe-mock`.

#### Complete Dockerfile: `services/payment/Dockerfile`

```dockerfile
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json ./
COPY tsconfig.base.json ./
COPY packages/contracts ./packages/contracts
COPY packages/shared ./packages/shared
COPY services/payment ./services/payment

RUN npm ci
RUN npm run build -w @eda/contracts
RUN npm run build -w @eda/shared
RUN npm run build -w @eda/payment

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

COPY package.json package-lock.json ./
COPY tsconfig.base.json ./
COPY packages/shared ./packages/shared
COPY mocks/stripe-mock ./mocks/stripe-mock

RUN npm ci
RUN npm run build -w @eda/shared
RUN npm run build -w @eda/stripe-mock

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
- `{ "type": "paymentSucceeded", ... }`
- `{ "type": "invoiceCreated", ... }`

### Step 12.6 — Verify broker state

- [ ] **RabbitMQ UI** (http://localhost:15672): queue `payment.requested` should have processed messages (Ready ≈ 0 after consumption).
- [ ] **Kafka UI** (http://localhost:8080): topics `paymentSucceeded` and `invoiceCreated` show new messages.
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
- [ ] **Dead letters:** Monitor `payment.requested.dlq`; alert and replay tooling
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

**Congratulations.** When all phases are checked off, you have a working event-driven architecture with an API gateway, six microservices, mock external providers, and production-oriented messaging infrastructure.

