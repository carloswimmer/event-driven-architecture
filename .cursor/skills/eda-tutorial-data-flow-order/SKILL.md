---
name: eda-tutorial-data-flow-order
description: >-
  Reorders Event-Driven Architecture tutorial steps (.cursor/tutorial.md) by
  runtime data flow instead of bottom-up scaffolding. Use when the user asks
  what order to implement phases or steps, wants a didactic learning path,
  mentions data flow order, "runtime first", or found the tutorial order
  unintuitive (e.g. Phase 6 gateway-before-handler).
---

# EDA Tutorial — Data-Flow Learning Order

## When to apply

The tutorial (`.cursor/tutorial.md`) orders steps **bottom-up**: adapters → domain → handlers → wiring. That is good for building libraries; it is **hard to learn from** because you implement infrastructure before you see a message move.

**Preferred learning order:** follow **runtime data flow** — entry point first, then each hop in the chain, stubs where needed, wire at the end.

> I'll do it in the order of data flow. This is the best way to learn.

Always read `.cursor/tutorial.md` for exact code. This skill only **reorders** tutorial step numbers.

## How to respond

When the user asks for learning order, output:

1. **One-line data flow** for the phase (or whole system if they ask broadly)
2. **Numbered learning steps** — each maps to a tutorial step (e.g. "Step 6.5")
3. **Stub note** — what can be empty/no-op until the next step
4. **Wire + build last** — module wiring and build steps stay at the end
5. **Checkpoint** — what they should be able to observe after each major hop

Do **not** rewrite tutorial code. Point to tutorial step anchors only.

## Global architecture (reference)

```text
POST /orders → api-gateway → RabbitMQ orders.payment.requested
  → payment → stripe-mock → webhook → Kafka orders.payment.succeeded
  → api-gateway (SSE) | availability | analytics | invoice
  → Kafka billing.invoice.created → analytics | notification → sendgrid-mock
```

## Phases that stay in tutorial order

These are foundation — no meaningful data-flow reorder:

| Phase | Why keep tutorial order |
|-------|-------------------------|
| **0** | Verify brokers/topics exist before any app |
| **1** | Monorepo + contracts package |
| **1.5** | Nest CLI + microservices deps |
| **2** | Shared helpers (idempotency, env, health) |
| **3** | Stripe mock — must exist before payment service calls it |
| **4** | SendGrid mock — must exist before notification service |
| **11** | Docker Compose wiring |
| **12** | End-to-end manual test |
| **13** | Production notes (read-only) |

**Macro service order (5→10)** in the tutorial already matches the global pipeline. Reorder **steps inside** each phase, not the phases themselves.

---

## Phase 5 — API Gateway (HTTP → RabbitMQ → Kafka → SSE)

**Data flow:** `POST /orders` → save order → publish `orders.payment.requested` → (later) Kafka `orders.payment.succeeded|failed` → update order → SSE push

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **5.1** | Scaffold, deps, `.env` | — |
| 1 | **5.2** | Contracts extension if needed | — |
| 2 | **5.3**, **5.3.1**, **5.4** | Order entity, Zod schema, repository | — |
| 3 | **5.5** | `OrderStatusStreamService` (register/watch/push) | — |
| 4 | **5.8** (orders controller only) | `POST /orders`, `GET /orders/:id` | `OrdersService` with save-only `createOrder` |
| 5 | **5.7** (part 1) | `createOrder`: save + `statusStream.register` | No RabbitMQ yet |
| 6 | **5.6** | Payment command publisher (RabbitMQ) | — |
| 7 | **5.7** (part 2) | `createOrder`: call `publishPaymentRequested` | — |
| 8 | **5.8** (orders-events controller) | `GET /orders/:id/events` SSE | — |
| 9 | **5.9** | Kafka `PaymentEventsHandler` | `applyPaymentResult` stub OK |
| 10 | **5.7** (part 3) | `applyPaymentResult` + `statusStream.push` | — |
| 11 | **5.10** | Wire modules + hybrid `main.ts` | — |
| 12 | **5.11** | Build and run | — |

**Checkpoint:** After step 7, `POST /orders` publishes to RabbitMQ. After step 10, a manual Kafka publish updates order status and SSE.

---

## Phase 6 — Payment Service (RabbitMQ → Stripe → webhook → Kafka)

**Data flow:** consume `orders.payment.requested` → call Stripe → webhook → emit `orders.payment.succeeded|failed`

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **6.1**, **6.1.1** | Scaffold, `.env`, Stripe webhook schema in `@eda/contracts`, rebuild contracts | — |
| 1 | **6.5** | `PaymentConsumerHandler` — validate, idempotency, ack, call service | `processPaymentRequested()` empty |
| 2 | **6.4** (part 1) | `processPaymentRequested` — log or no-op | No gateway yet |
| 3 | **6.2** | `PaymentGateway` + `StripePaymentGateway` | — |
| 4 | **6.4** (part 2) | `processPaymentRequested` → `paymentGateway.createPaymentIntent` | Webhook + Kafka stubbed |
| 5 | **6.6** | `WebhooksController` — validate body, call service | `handleStripeWebhook` empty |
| 6 | **6.4** (part 3) | `handleStripeWebhook` — branch on `type`, no publish yet | — |
| 7 | **6.3** | `DomainEventPublisher` + `KafkaDomainEventPublisher` | — |
| 8 | **6.4** (complete) | Wire publisher in webhook path; `publishPaymentFailed` for gateway errors | — |
| 9 | **6.7** | Wire `PaymentModule`, hybrid `main.ts` (RabbitMQ consumer + HTTP) | — |
| 10 | **6.8** | Build | — |

**Checkpoint:** After 4, RabbitMQ message reaches Stripe mock. After 8, full loop through webhook to Kafka.

---

## Phase 7 — Availability (Kafka → inventory)

**Data flow:** Kafka `orders.payment.succeeded` → confirm reservation

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **7.1** | Scaffold, `.env` | — |
| 1 | **7.4** | `PaymentEventsHandler` — parse + idempotency | `confirmReservation` stub |
| 2 | **7.2** | Inventory repository | — |
| 3 | **7.3** | `InventoryService.confirmReservation` | — |
| 4 | **7.4** (complete) | Handler calls service | — |
| 5 | **7.5**, **7.6** | Wire + hybrid main + build | — |

---

## Phase 8 — Analytics (Kafka → store → HTTP read)

**Data flow:** Kafka events → append to store → (debug) `GET /events`

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **8.1** | Scaffold, `.env` | — |
| 1 | **8.3** | `KafkaEventsHandler` — parse + log | `EventsService.record` stub |
| 2 | **8.2** (repo + service) | Repository + `EventsService.record` | — |
| 3 | **8.3** (complete) | Handlers call `record` | — |
| 4 | **8.2** (controller) | `GET /events` — read path (optional to test after handlers work) | — |
| 5 | **8.4**, **8.5** | Wire + hybrid main + build | — |

---

## Phase 9 — Invoice (Kafka in → invoice → Kafka out)

**Data flow:** Kafka `orders.payment.succeeded` → create invoice → Kafka `billing.invoice.created`

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **9.1** | Scaffold, `.env` | — |
| 1 | **9.4** | `PaymentEventsHandler` — parse + idempotency | `createFromPayment` stub |
| 2 | **9.3** (part 1) | `InvoiceService.createFromPayment` — log only | No publisher |
| 3 | **9.2** | Domain event publisher (Kafka producer) | — |
| 4 | **9.3** (complete) | Service publishes `billing.invoice.created` | — |
| 5 | **9.4** (complete) | Handler calls service | — |
| 6 | **9.5**, **9.6** | Wire + hybrid main + build | — |

---

## Phase 10 — Notification (Kafka → SendGrid)

**Data flow:** Kafka `billing.invoice.created` → send email via SendGrid mock

| # | Tutorial step | What you build | Stub until next |
|---|---------------|----------------|-----------------|
| 0 | **10.1** | Scaffold, `.env` | — |
| 1 | **10.4** | `InvoiceEventsHandler` — parse + idempotency | `sendInvoiceNotification` stub |
| 2 | **10.3** | `NotificationService` — log recipient | No gateway |
| 3 | **10.2** | `EmailGateway` + `SendGridEmailGateway` | — |
| 4 | **10.3** (complete) | Service calls gateway | — |
| 5 | **10.4** (complete) | Handler calls service | — |
| 6 | **10.5**, **10.6** | Wire + hybrid main + build all | — |

---

## Pattern (reuse for any phase)

When the user is on a **new phase** not listed above, derive order with:

1. **Scaffold + contracts** (always first)
2. **Inbound handler** — the `@EventPattern`, `@Post`, or consumer that receives the message
3. **Domain service** — what the handler calls (stub external deps)
4. **Outbound adapters** — gateway (HTTP), publisher (Kafka/RabbitMQ)
5. **Complete domain service** — wire adapters, error paths, idempotency marks
6. **Complete handler** — ack/nack, dedup, logging
7. **Secondary read paths** — HTTP GET, SSE (if not the primary entry)
8. **Wire modules + main.ts + build**

## Stub rules

- Stubs are **intentional** — one line log or empty `Promise.resolve()` is fine
- Replace stubs **in place** in the same file; do not create parallel "stub" files
- **Idempotency + ack/nack** belong with the inbound handler once the happy path exists
- **Never** skip wire/build — hybrid apps need `main.ts` microservice connection before runtime tests

## Example response (Phase 6)

```markdown
## Phase 6 — learning order (data flow)

`RabbitMQ command → PaymentService → Stripe → webhook → Kafka event`

1. **6.1 + 6.1.1** — scaffold + contracts
2. **6.5** — consumer handler (stub service)
3. **6.4** — `processPaymentRequested` only
4. **6.2** — Stripe gateway
5. **6.6** — webhook controller
6. **6.4** — `handleStripeWebhook`
7. **6.3** — Kafka publisher
8. **6.4** — failure path + full service
9. **6.7 + 6.8** — wire + build

After step 4 you can trace a command to Stripe. After step 7 the full loop emits to Kafka.
```

## Additional reference

For step-to-file mapping and architecture diagram, see [tutorial.md](../../tutorial.md) Phase headers and the mermaid diagram at the top.
