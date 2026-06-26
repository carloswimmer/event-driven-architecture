# Payment Service — Design Spec (Phase 6)

**Date:** 2026-06-24  
**Status:** Approved  
**Approach:** Curated learning spec — tutorial base + explicit improvements documented

---

## 1. Purpose and boundaries

The **Payment Service** (`services/payment/`, port **3010**) is the worker that:

1. **Consumes** the command `orders.payment.requested` (RabbitMQ)
2. **Orchestrates** PaymentIntent creation via the Stripe mock (HTTP outbound)
3. **Receives** the asynchronous webhook from stripe-mock (HTTP inbound)
4. **Publishes** domain events `orders.payment.succeeded` or `orders.payment.failed` (Kafka)

It has no persistent business state — it is a **stateless orchestrator** with in-memory idempotency to prevent reprocessing.

### Position in the flow

```
API Gateway → RabbitMQ (orders.payment.requested) → Payment Service
Payment Service → stripe-mock (POST /v1/payment-intents)
stripe-mock → Payment Service (POST /webhooks/stripe)
Payment Service → Kafka (orders.payment.succeeded | orders.payment.failed)
Kafka → API Gateway, Availability, Analytics, Invoice (parallel fan-out)
```

### Prerequisites (already implemented)

| Layer | Status |
|-------|--------|
| RabbitMQ + Kafka infrastructure | Ready |
| `packages/contracts` — `PaymentRequested`, `PaymentSucceeded`, `PaymentFailed` | Ready |
| `mocks/stripe-mock` | Ready |
| `services/api-gateway` — publishes command, consumes Kafka events | Ready |

---

## 2. Architecture

```mermaid
flowchart TB
  subgraph inbound["PaymentInboundModule"]
    RMQ["PaymentConsumerHandler\n(RabbitMQ)"]
    WH["WebhooksController\n(HTTP)"]
  end

  subgraph core["PaymentModule"]
    PS["PaymentService"]
    GW["StripePaymentGateway"]
    PUB["KafkaDomainEventPublisher"]
    IDEM["IdempotencyStore"]
  end

  RMQ --> PS
  WH --> PS
  PS --> GW
  PS --> PUB
  RMQ --> IDEM
  WH --> IDEM
  GW -->|POST /v1/payment-intents| StripeMock
  StripeMock -->|POST /webhooks/stripe| WH
  PUB --> Kafka
```

### Design decisions

| Topic | Decision | Rationale |
|-------|----------|-----------|
| Base | Tutorial Phase 6 + documented improvements | Learning value without over-engineering |
| Stripe errors | Fail-fast → `payment_failed` + ack | Simple, predictable; no DLQ retry |
| Deduplication | Symmetric — RabbitMQ + webhook | Prevents duplicate Kafka events from webhook replays |
| Module split | `PaymentModule` + `PaymentInboundModule` | Separates inbound adapters from domain logic |
| Idempotency store | In-memory (`IdempotencyStore`) | Matches api-gateway pattern; sufficient for local study |

---

## 3. DTOs and contracts (`@eda/contracts`)

All payload names are defined once in `@eda/contracts`. Services validate with `*Schema.parse(...)` at boundaries. Never hardcode routing keys or topic names.

### Existing schemas (ready)

| Schema | Direction | Transport | Fields |
|--------|-----------|-----------|--------|
| `PaymentRequested` | Inbound | RabbitMQ `orders.payment.requested` | `reserveId`, `orderNumber`, `amount`, `customerEmail` |
| `PaymentSucceeded` | Outbound | Kafka `orders.payment.succeeded` | `reserveId`, `value`, `customerInfo{email}`, `orderNumber` |
| `PaymentFailed` | Outbound | Kafka `orders.payment.failed` | `orderNumber`, `reason` |

### New schema (Step 6.1.1)

**File:** `packages/contracts/src/events/stripe-webhook.ts`

```typescript
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
```

Export from `packages/contracts/src/index.ts` and rebuild contracts.

### Webhook → Kafka mapping

| Webhook type | Kafka topic | Payload |
|--------------|-------------|---------|
| `payment_intent.succeeded` | `orders.payment.succeeded` | `{ reserveId, value: amount, customerInfo: { email: customerEmail }, orderNumber }` |
| `payment_intent.payment_failed` | `orders.payment.failed` | `{ orderNumber, reason: 'payment_intent.payment_failed' }` |

### Stripe HTTP failure (no webhook)

When `StripePaymentGateway.createPaymentIntent` throws or returns non-OK:

```typescript
{ orderNumber, reason: 'stripe_error' }
```

Published to `orders.payment.failed` before RabbitMQ ack.

---

## 4. NestJS module map

### Project layout

Standalone Nest project at `services/payment/` — same pattern as `services/api-gateway` (own `package.json`, `nest-cli.json`, `biome.json`, `node_modules`; not registered in root pnpm workspace as an app).

```text
services/payment/src/
├── main.ts
├── app.module.ts
├── common/                          # copied from api-gateway
│   ├── env.ts
│   ├── health.controller.ts
│   ├── idempotency.store.ts
│   └── zod-validation.filter.ts
├── payment/
│   ├── payment.service.ts
│   ├── payment.service.spec.ts
│   └── payment.module.ts
├── payment-consumer/
│   ├── payment-consumer.handler.ts
│   └── payment-consumer.handler.spec.ts
├── webhooks/
│   ├── webhooks.controller.ts
│   └── webhooks.controller.spec.ts
├── gateways/
│   ├── payment.gateway.ts
│   └── stripe-payment.gateway.ts
├── messaging/
│   ├── domain-event.publisher.ts
│   └── kafka-domain-event.publisher.ts
└── payment-inbound/
    └── payment-inbound.module.ts
```

### Module wiring

```
AppModule
├── imports: [PaymentModule, PaymentInboundModule]
└── controllers: [HealthController]

PaymentModule                          # domain + outbound ports
├── imports: [ClientsModule.register(KAFKA_SERVICE)]
├── providers:
│   ├── PaymentService
│   ├── IdempotencyStore
│   ├── { PAYMENT_GATEWAY → StripePaymentGateway }
│   └── { DOMAIN_EVENT_PUBLISHER → KafkaDomainEventPublisher }
└── exports: [PaymentService, IdempotencyStore]

PaymentInboundModule                   # thin inbound adapters
├── imports: [PaymentModule]
├── controllers:
│   ├── PaymentConsumerHandler         # @EventPattern(ROUTING_KEYS.PAYMENT_REQUESTED)
│   └── WebhooksController             # POST /webhooks/stripe
└── providers: []                      # delegates to PaymentService
```

### Ports (interfaces + Symbol tokens)

| Port | Token | Implementation | Responsibility |
|------|-------|----------------|----------------|
| `PaymentGateway` | `PAYMENT_GATEWAY` | `StripePaymentGateway` | `createPaymentIntent(PaymentRequested)` → HTTP POST to stripe-mock |
| `DomainEventPublisher` | `DOMAIN_EVENT_PUBLISHER` | `KafkaDomainEventPublisher` | `publishPaymentSucceeded` / `publishPaymentFailed` → Kafka emit |

### Hybrid bootstrap (`main.ts`)

```typescript
NestFactory.create(AppModule, FastifyAdapter)
app.useGlobalFilters(new ZodValidationExceptionFilter())
app.connectMicroservice({
  transport: Transport.RMQ,
  urls: [requireEnv('RABBITMQ_URL')],
  queue: 'orders.payment.requested',
  noAck: false,
  prefetchCount: 1,
  queueOptions: { durable: true },
  wildcards: true,
  exchange: EXCHANGES.COMMANDS,
  exchangeType: 'topic',
})
await app.startAllMicroservices()
await app.listen(PORT, '0.0.0.0')
```

---

## 5. Messaging strategy

### 5.1 RabbitMQ — command consumer

| Config | Value |
|--------|-------|
| Exchange | `eda.commands` (topic) |
| Queue | `orders.payment.requested` (quorum, DLX → `eda.dlx`) |
| Routing key | `orders.payment.requested` (`ROUTING_KEYS.PAYMENT_REQUESTED`) |
| Ack mode | `noAck: false` — manual ack/nack |
| Prefetch | `1` |

**Handler flow (`PaymentConsumerHandler`):**

```
payload
  → PaymentRequestedSchema.parse()
  → idempotency.isDuplicate(orderNumber)?
       yes → log warn → ack → return
  → paymentService.processPaymentRequested(data)
       catch → publishPaymentFailed(orderNumber, 'stripe_error')
  → idempotency.markProcessed(orderNumber)
  → channel.ack(msg)

parse failure or unexpected crash → channel.nack(msg, false, false) → DLQ
```

### 5.2 Kafka — event producer

| Config | Value |
|--------|-------|
| Client ID | `payment-service` |
| Topics | `orders.payment.succeeded`, `orders.payment.failed` |
| Auto-create topics | `false` |
| Role in Phase 6 | Producer only (no consumer) |

Emit via `ClientKafka.emit(TOPICS.*, payload)` after `*Schema.parse(...)`.

### 5.3 HTTP — webhook inbound

| Endpoint | Body | Response |
|----------|------|----------|
| `POST /webhooks/stripe` | `StripeWebhook` (validated with Zod) | `{ received: true }` |

Configure stripe-mock with `PAYMENT_WEBHOOK_URL=http://localhost:3010/webhooks/stripe`.

### 5.4 Idempotency (symmetric dedup — improvement over tutorial)

Uses the shared `IdempotencyStore` (in-memory `Set<string>`).

| Entry point | Key | On duplicate |
|-------------|-----|--------------|
| RabbitMQ command | `orderNumber` | ack, skip processing |
| Stripe webhook | `` `${orderNumber}:${event.type}` `` | return `{ received: true }`, skip Kafka publish |

Example webhook key: `abc-uuid:payment_intent.succeeded`

---

## 6. Error handling (fail-fast)

| Scenario | RabbitMQ | Kafka | Idempotency |
|----------|----------|-------|-------------|
| Invalid payload (Zod) | `nack` → DLQ | — | not marked |
| Duplicate command | `ack` | — | already marked |
| Stripe HTTP failure | `ack` | `payment_failed` (`stripe_error`) | mark `orderNumber` |
| Stripe OK, await webhook | `ack` | — (webhook publishes) | mark `orderNumber` |
| Webhook `succeeded` | — | `payment_succeeded` | mark `${orderNumber}:type` |
| Webhook `failed` | — | `payment_failed` | mark `${orderNumber}:type` |
| Duplicate webhook | — | skip publish | already marked |

No automatic retry for transient Stripe failures — the order is definitively marked as failed via Kafka.

---

## 7. Environment variables

**File:** `services/payment/.env`

```bash
PORT=3010
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
KAFKA_BROKERS=localhost:9094
STRIPE_MOCK_URL=http://localhost:3001
```

### Note on `RABBITMQ_URL` format

The root `.env.example` exposes RabbitMQ credentials as separate variables (`RABBITMQ_APP_USER`, `RABBITMQ_APP_PASS`). Microservices use a composed `RABBITMQ_URL` string — same pattern as `services/api-gateway/.env.example`. Docker Compose (Phase 11) uses host `rabbitmq` instead of `localhost`.

**For mock-based tests:** the concrete `RABBITMQ_URL` value is irrelevant — unit, handler/controller, and e2e tests do not connect to a real broker. It only matters when the process bootstraps `connectMicroservice` against live infrastructure.

---

## 8. Testing strategy

### 8.1 Unit — `PaymentService`

**File:** `src/payment/payment.service.spec.ts`

Mock `PAYMENT_GATEWAY` and `DOMAIN_EVENT_PUBLISHER` via Nest testing module.

| Scenario | Assert |
|----------|--------|
| `processPaymentRequested` | calls `createPaymentIntent` with correct payload |
| Stripe failure path | calls `publishPaymentFailed` with `reason: 'stripe_error'` |
| Webhook `payment_intent.succeeded` | publishes mapped `PaymentSucceeded` |
| Webhook `payment_intent.payment_failed` | publishes `PaymentFailed` with correct reason |

### 8.2 Handler/controller — thin adapters

**File:** `src/payment-consumer/payment-consumer.handler.spec.ts`

Mock `PaymentService`, `IdempotencyStore`, and `RmqContext` (channel `ack`/`nack` spies).

| Scenario | Assert |
|----------|--------|
| Invalid payload | `nack(msg, false, false)`; service not called |
| Duplicate `orderNumber` | `ack`; service not called |
| Happy path | `processPaymentRequested` called → `ack` |
| Stripe failure (fail-fast) | `publishPaymentFailed('stripe_error')` → `ack` |
| Unexpected crash after parse | `nack` → DLQ |

**File:** `src/webhooks/webhooks.controller.spec.ts`

| Scenario | Assert |
|----------|--------|
| Valid `succeeded` body | delegates to `handleStripeWebhook`; returns `{ received: true }` |
| Invalid body (Zod) | 400 via `ZodValidationExceptionFilter` |
| Duplicate webhook (dedup) | returns `{ received: true }`; publisher not called again |

### 8.3 E2E — HTTP webhook (mocked infra)

**File:** `test/payment.e2e-spec.ts`

Bootstrap Fastify app **without** `connectMicroservice`, or use a test module that overrides Kafka/RabbitMQ providers.

| Scenario | Assert |
|----------|--------|
| `GET /health` | 200 |
| `POST /webhooks/stripe` valid payload | 200 + `{ received: true }` + mock publisher receives event |
| `POST /webhooks/stripe` invalid payload | 400 |

Runs in CI without Docker.

### 8.4 Out of scope (documented for future)

- Integration e2e with real RabbitMQ + Kafka + stripe-mock (Phase 12 manual test)
- Integration tests for `StripePaymentGateway` / `KafkaDomainEventPublisher` against real HTTP/brokers

---

## 9. Documented improvements (not implemented in Phase 6)

| Gap | Impact | Recommended future solution |
|-----|--------|----------------------------|
| In-memory idempotency | State lost on restart; no horizontal scaling | Redis or DB with TTL |
| Fail-fast without retry | Transient failure = definitive payment failure | Retry with backoff + DLQ |
| No Kafka message key | Events for same order may land on different partitions | Set `key: orderNumber` on emit |
| No webhook signature verification | Any POST to `/webhooks/stripe` is accepted | Validate `Stripe-Signature` HMAC |
| No outbox pattern | Kafka emit may fail after RabbitMQ ack | Transactional outbox |
| Downstream consumers without dedup | Duplicate Kafka delivery possible | Idempotent consumers in api-gateway and downstream services |

---

## 10. Checkpoint criteria

- [ ] `services/payment/` builds from its own folder (`pnpm build`)
- [ ] `packages/contracts` includes `StripeWebhookSchema` and rebuilds
- [ ] Flow: RabbitMQ handler → Zod → `PaymentService` → `PaymentGateway`
- [ ] Flow: Stripe webhook → Zod → dedup → `DomainEventPublisher` → Zod → Kafka
- [ ] Unit tests pass for `PaymentService`
- [ ] Handler/controller tests pass
- [ ] E2E webhook tests pass (mocked infra)
- [ ] Manual: stripe-mock webhook reaches `POST /webhooks/stripe` when service is running on 3010

### Suggested commit message

```
feat: add payment service with zod validation, gateway, and event publisher ports
```

---

## 11. Decision log

| Question | Answer |
|----------|--------|
| Spec approach | C — tutorial base + explicit improvements |
| Stripe error semantics | A — fail-fast (Kafka `payment_failed` + ack) |
| Deduplication | B — symmetric (RabbitMQ + webhook) |
| Module structure | C — `PaymentModule` + `PaymentInboundModule` |
| Tests | Unit + handler/controller + e2e HTTP (mocked infra) |
