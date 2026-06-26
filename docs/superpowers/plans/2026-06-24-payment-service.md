# Payment Service (Phase 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Payment Service as a standalone NestJS hybrid app that consumes `orders.payment.requested` from RabbitMQ, calls stripe-mock, receives webhooks, and publishes payment events to Kafka.

**Architecture:** Curated learning spec — tutorial Phase 6 base with `PaymentModule` (domain + outbound ports) and `PaymentInboundModule` (thin RabbitMQ/HTTP adapters), symmetric in-memory idempotency, fail-fast Stripe errors. Contracts live in `@eda/contracts`; validate every boundary with Zod.

**Tech Stack:** NestJS 10, Fastify, `@nestjs/microservices`, RabbitMQ, Kafka (kafkajs), Zod, Biome, Jest, `@eda/contracts` (file link)

**Spec reference:** [`docs/superpowers/specs/2026-06-24-payment-service-design.md`](../specs/2026-06-24-payment-service-design.md)

---

## File map

| File | Responsibility |
|------|----------------|
| `packages/contracts/src/events/stripe-webhook.ts` | Stripe webhook Zod schema (new) |
| `packages/contracts/src/index.ts` | Export stripe-webhook |
| `services/payment/` | Standalone Nest project scaffold |
| `services/payment/src/common/*` | env, health, idempotency, zod filter (copied from api-gateway) |
| `services/payment/src/gateways/payment.gateway.ts` | Outbound port interface |
| `services/payment/src/gateways/stripe-payment.gateway.ts` | HTTP adapter to stripe-mock |
| `services/payment/src/messaging/domain-event.publisher.ts` | Kafka publish port interface |
| `services/payment/src/messaging/kafka-domain-event.publisher.ts` | Kafka adapter |
| `services/payment/src/payment/payment.service.ts` | Domain orchestration |
| `services/payment/src/payment-consumer/payment-consumer.handler.ts` | RabbitMQ thin adapter |
| `services/payment/src/webhooks/webhooks.controller.ts` | HTTP webhook thin adapter + dedup |
| `services/payment/src/payment/payment.module.ts` | Domain module wiring |
| `services/payment/src/payment-inbound/payment-inbound.module.ts` | Inbound adapters module |
| `services/payment/src/app.module.ts` | Root module |
| `services/payment/src/main.ts` | Hybrid bootstrap (RMQ + HTTP) |
| `services/payment/test/payment.e2e-spec.ts` | E2E webhook tests (mocked infra) |

---

### Task 1: Add `StripeWebhook` contract

**Files:**
- Create: `packages/contracts/src/events/stripe-webhook.ts`
- Modify: `packages/contracts/src/index.ts`

- [ ] **Step 1: Create stripe-webhook schema**

Create `packages/contracts/src/events/stripe-webhook.ts`:

```typescript
import { z } from 'zod'

export const StripeWebhookDataSchema = z.object({
	orderNumber: z.string().uuid(),
	amount: z.number().positive(),
	reserveId: z.string().uuid(),
	customerEmail: z.string().email(),
})

export const StripeWebhookSchema = z.discriminatedUnion('type', [
	z.object({
		type: z.literal('payment_intent.succeeded'),
		data: StripeWebhookDataSchema,
	}),
	z.object({
		type: z.literal('payment_intent.payment_failed'),
		data: StripeWebhookDataSchema,
	}),
])

export type StripeWebhook = z.infer<typeof StripeWebhookSchema>
```

- [ ] **Step 2: Export from contracts index**

Add to `packages/contracts/src/index.ts`:

```typescript
export * from './events/stripe-webhook'
```

- [ ] **Step 3: Build contracts**

Run (from repository root):

```bash
pnpm --filter @eda/contracts build
```

Expected: build succeeds with no TypeScript errors.

- [ ] **Step 4: Commit**

```bash
git add packages/contracts/src/events/stripe-webhook.ts packages/contracts/src/index.ts
git commit -m "feat(contracts): add StripeWebhook schema for payment service webhooks"
```

---

### Task 2: Scaffold standalone Payment Service project

**Files:**
- Create: `services/payment/**` (Nest CLI scaffold + config)

- [ ] **Step 1: Generate Nest project**

Run from repository root:

```bash
mkdir -p services
cd services
npx nest new payment --package-manager pnpm --strict --skip-git
```

Expected: `services/payment/` with own `package.json`, `nest-cli.json`, `tsconfig.json`, `src/`.

- [ ] **Step 2: Swap ESLint/Prettier for Biome**

Run from `services/payment/`:

```bash
rm -f .eslintrc.js .prettierrc
pnpm remove eslint @typescript-eslint/eslint-plugin @typescript-eslint/parser eslint-config-prettier eslint-plugin-prettier prettier
pnpm add -D @biomejs/biome@2.5.0 --save-exact
```

Create `services/payment/biome.json`:

```json
{
	"$schema": "https://biomejs.dev/schemas/2.5.0/schema.json",
	"root": false,
	"extends": ["../../biome.json"]
}
```

Update `services/payment/package.json` scripts:

```json
"format": "biome format --write .",
"lint": "biome check .",
"lint:fix": "biome check --write ."
```

- [ ] **Step 3: Install dependencies**

Run from `services/payment/`:

```bash
pnpm add @nestjs/platform-fastify @nestjs/microservices amqplib amqp-connection-manager kafkajs dotenv zod
pnpm add -D @types/amqplib
pnpm add @eda/contracts@file:../../packages/contracts
```

- [ ] **Step 4: Copy common helpers from api-gateway**

Run from `services/payment/`:

```bash
cp -r ../api-gateway/src/common ./src/common
```

- [ ] **Step 5: Create env files**

Create `services/payment/.env`:

```bash
PORT=3010
RABBITMQ_URL=amqp://eda_app:change-me-app-password@localhost:5672/eda
KAFKA_BROKERS=localhost:9094
STRIPE_MOCK_URL=http://localhost:3001
```

Create `services/payment/.env.example` (same content as `.env`).

- [ ] **Step 6: Scaffold feature folders**

Run from `services/payment/`:

```bash
npx nest g module payment --no-spec
npx nest g service payment --no-spec
npx nest g controller payment-consumer --no-spec
npx nest g controller webhooks --no-spec
mkdir -p src/gateways src/messaging src/payment-inbound
```

- [ ] **Step 7: Verify scaffold builds**

Run from `services/payment/`:

```bash
pnpm build
```

Expected: build succeeds (default Nest scaffold).

- [ ] **Step 8: Commit**

```bash
git add services/payment
git commit -m "chore: scaffold payment service standalone Nest project"
```

---

### Task 3: Payment gateway port + Stripe adapter

**Files:**
- Create: `services/payment/src/gateways/payment.gateway.ts`
- Create: `services/payment/src/gateways/stripe-payment.gateway.ts`

- [ ] **Step 1: Create port interface**

Create `services/payment/src/gateways/payment.gateway.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts'

export const PAYMENT_GATEWAY = Symbol('PAYMENT_GATEWAY')

export interface PaymentGateway {
	createPaymentIntent(input: PaymentRequested): Promise<void>
}
```

- [ ] **Step 2: Create Stripe adapter**

Create `services/payment/src/gateways/stripe-payment.gateway.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts'
import { Injectable } from '@nestjs/common'
import { requireEnv } from '../common/env'
import { PaymentGateway } from './payment.gateway'

@Injectable()
export class StripePaymentGateway implements PaymentGateway {
	private readonly baseUrl = requireEnv('STRIPE_MOCK_URL')

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
		})

		if (!response.ok) {
			throw new Error(`Stripe mock returned HTTP ${response.status}`)
		}
	}
}
```

- [ ] **Step 3: Commit**

```bash
git add services/payment/src/gateways
git commit -m "feat(payment): add PaymentGateway port and Stripe adapter"
```

---

### Task 4: Domain event publisher port + Kafka adapter

**Files:**
- Create: `services/payment/src/messaging/domain-event.publisher.ts`
- Create: `services/payment/src/messaging/kafka-domain-event.publisher.ts`

- [ ] **Step 1: Create port interface**

Create `services/payment/src/messaging/domain-event.publisher.ts`:

```typescript
import { PaymentFailed, PaymentSucceeded } from '@eda/contracts'

export const DOMAIN_EVENT_PUBLISHER = Symbol('DOMAIN_EVENT_PUBLISHER')

export interface DomainEventPublisher {
	publishPaymentSucceeded(event: PaymentSucceeded): Promise<void>
	publishPaymentFailed(event: PaymentFailed): Promise<void>
}
```

- [ ] **Step 2: Create Kafka adapter**

Create `services/payment/src/messaging/kafka-domain-event.publisher.ts`:

```typescript
import {
	PaymentFailed,
	PaymentFailedSchema,
	PaymentSucceeded,
	PaymentSucceededSchema,
	TOPICS,
} from '@eda/contracts'
import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common'
import { ClientKafka, ClientKafkaProxy } from '@nestjs/microservices'
import { firstValueFrom } from 'rxjs'
import { DomainEventPublisher } from './domain-event.publisher'

@Injectable()
export class KafkaDomainEventPublisher
	implements DomainEventPublisher, OnModuleInit, OnModuleDestroy
{
	constructor(
		@Inject('KAFKA_SERVICE') private readonly kafka: ClientKafkaProxy,
	) {}

	async onModuleInit() {
		await (this.kafka as ClientKafka).connect()
	}

	async publishPaymentSucceeded(event: PaymentSucceeded): Promise<void> {
		const payload = PaymentSucceededSchema.parse(event)
		await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_SUCCEEDED, payload))
	}

	async publishPaymentFailed(event: PaymentFailed): Promise<void> {
		const payload = PaymentFailedSchema.parse(event)
		await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_FAILED, payload))
	}

	async onModuleDestroy() {
		await (this.kafka as ClientKafka).close()
	}
}
```

- [ ] **Step 3: Commit**

```bash
git add services/payment/src/messaging
git commit -m "feat(payment): add DomainEventPublisher port and Kafka adapter"
```

---

### Task 5: PaymentService — TDD unit tests

**Files:**
- Create: `services/payment/src/payment/payment.service.spec.ts`
- Modify: `services/payment/src/payment/payment.service.ts`

- [ ] **Step 1: Write failing unit tests**

Create `services/payment/src/payment/payment.service.spec.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts'
import { Test, TestingModule } from '@nestjs/testing'
import { PAYMENT_GATEWAY, PaymentGateway } from '../gateways/payment.gateway'
import {
	DOMAIN_EVENT_PUBLISHER,
	DomainEventPublisher,
} from '../messaging/domain-event.publisher'
import { PaymentService } from './payment.service'

describe('PaymentService', () => {
	let service: PaymentService
	let gateway: jest.Mocked<PaymentGateway>
	let events: jest.Mocked<DomainEventPublisher>

	const paymentRequested: PaymentRequested = {
		reserveId: '11111111-1111-4111-8111-111111111111',
		orderNumber: '22222222-2222-4222-8222-222222222222',
		amount: 99.9,
		customerEmail: 'buyer@example.com',
	}

	beforeEach(async () => {
		gateway = { createPaymentIntent: jest.fn().mockResolvedValue(undefined) }
		events = {
			publishPaymentSucceeded: jest.fn().mockResolvedValue(undefined),
			publishPaymentFailed: jest.fn().mockResolvedValue(undefined),
		}

		const module: TestingModule = await Test.createTestingModule({
			providers: [
				PaymentService,
				{ provide: PAYMENT_GATEWAY, useValue: gateway },
				{ provide: DOMAIN_EVENT_PUBLISHER, useValue: events },
			],
		}).compile()

		service = module.get(PaymentService)
	})

	it('processPaymentRequested calls gateway with payload', async () => {
		await service.processPaymentRequested(paymentRequested)

		expect(gateway.createPaymentIntent).toHaveBeenCalledWith(paymentRequested)
	})

	it('publishPaymentFailed delegates to event publisher', async () => {
		await service.publishPaymentFailed(
			paymentRequested.orderNumber,
			'stripe_error',
		)

		expect(events.publishPaymentFailed).toHaveBeenCalledWith({
			orderNumber: paymentRequested.orderNumber,
			reason: 'stripe_error',
		})
	})

	it('handleStripeWebhook succeeded maps and publishes PaymentSucceeded', async () => {
		await service.handleStripeWebhook({
			type: 'payment_intent.succeeded',
			data: {
				orderNumber: paymentRequested.orderNumber,
				amount: paymentRequested.amount,
				reserveId: paymentRequested.reserveId,
				customerEmail: paymentRequested.customerEmail,
			},
		})

		expect(events.publishPaymentSucceeded).toHaveBeenCalledWith({
			reserveId: paymentRequested.reserveId,
			value: paymentRequested.amount,
			customerInfo: { email: paymentRequested.customerEmail },
			orderNumber: paymentRequested.orderNumber,
		})
	})

	it('handleStripeWebhook failed publishes PaymentFailed', async () => {
		await service.handleStripeWebhook({
			type: 'payment_intent.payment_failed',
			data: {
				orderNumber: paymentRequested.orderNumber,
				amount: paymentRequested.amount,
				reserveId: paymentRequested.reserveId,
				customerEmail: paymentRequested.customerEmail,
			},
		})

		expect(events.publishPaymentFailed).toHaveBeenCalledWith({
			orderNumber: paymentRequested.orderNumber,
			reason: 'payment_intent.payment_failed',
		})
	})
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run from `services/payment/`:

```bash
pnpm test -- payment.service.spec.ts
```

Expected: FAIL — `PaymentService` methods missing or not implemented.

- [ ] **Step 3: Implement PaymentService**

Replace `services/payment/src/payment/payment.service.ts`:

```typescript
import {
	PaymentRequested,
	PaymentSucceededSchema,
	StripeWebhook,
} from '@eda/contracts'
import { Inject, Injectable } from '@nestjs/common'
import { PAYMENT_GATEWAY, PaymentGateway } from '../gateways/payment.gateway'
import {
	DOMAIN_EVENT_PUBLISHER,
	DomainEventPublisher,
} from '../messaging/domain-event.publisher'

@Injectable()
export class PaymentService {
	constructor(
		@Inject(PAYMENT_GATEWAY) private readonly paymentGateway: PaymentGateway,
		@Inject(DOMAIN_EVENT_PUBLISHER)
		private readonly events: DomainEventPublisher,
	) {}

	async processPaymentRequested(data: PaymentRequested): Promise<void> {
		await this.paymentGateway.createPaymentIntent(data)
	}

	async publishPaymentFailed(orderNumber: string, reason: string): Promise<void> {
		await this.events.publishPaymentFailed({ orderNumber, reason })
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
			)
			return
		}

		if (body.type === 'payment_intent.payment_failed') {
			await this.events.publishPaymentFailed({
				orderNumber: body.data.orderNumber,
				reason: 'payment_intent.payment_failed',
			})
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run from `services/payment/`:

```bash
pnpm test -- payment.service.spec.ts
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/payment/src/payment/payment.service.ts services/payment/src/payment/payment.service.spec.ts
git commit -m "feat(payment): implement PaymentService with unit tests"
```

---

### Task 6: PaymentConsumerHandler — TDD

**Files:**
- Delete: `services/payment/src/payment-consumer/payment-consumer.controller.ts` (if created by CLI)
- Create: `services/payment/src/payment-consumer/payment-consumer.handler.ts`
- Create: `services/payment/src/payment-consumer/payment-consumer.handler.spec.ts`

- [ ] **Step 1: Write failing handler tests**

Create `services/payment/src/payment-consumer/payment-consumer.handler.spec.ts`:

```typescript
import { PaymentRequested } from '@eda/contracts'
import { RmqContext } from '@nestjs/microservices'
import { IdempotencyStore } from '../common/idempotency.store'
import { PaymentService } from '../payment/payment.service'
import { PaymentConsumerHandler } from './payment-consumer.handler'

describe('PaymentConsumerHandler', () => {
	let handler: PaymentConsumerHandler
	let idempotency: IdempotencyStore
	let paymentService: jest.Mocked<
		Pick<PaymentService, 'processPaymentRequested' | 'publishPaymentFailed'>
	>
	let ack: jest.Mock
	let nack: jest.Mock
	let context: RmqContext

	const validPayload: PaymentRequested = {
		reserveId: '11111111-1111-4111-8111-111111111111',
		orderNumber: '22222222-2222-4222-8222-222222222222',
		amount: 50,
		customerEmail: 'buyer@example.com',
	}

	beforeEach(() => {
		idempotency = new IdempotencyStore()
		paymentService = {
			processPaymentRequested: jest.fn().mockResolvedValue(undefined),
			publishPaymentFailed: jest.fn().mockResolvedValue(undefined),
		}
		handler = new PaymentConsumerHandler(idempotency, paymentService as PaymentService)

		ack = jest.fn()
		nack = jest.fn()
		context = {
			getChannelRef: () => ({ ack, nack }),
			getMessage: () => ({ content: Buffer.from('{}') }),
		} as unknown as RmqContext
	})

	it('nacks invalid payload', async () => {
		await handler.handlePaymentRequested({ bad: true }, context)

		expect(nack).toHaveBeenCalledWith({ content: Buffer.from('{}') }, false, false)
		expect(paymentService.processPaymentRequested).not.toHaveBeenCalled()
	})

	it('acks duplicate orderNumber without calling service', async () => {
		idempotency.markProcessed(validPayload.orderNumber)

		await handler.handlePaymentRequested(validPayload, context)

		expect(ack).toHaveBeenCalled()
		expect(paymentService.processPaymentRequested).not.toHaveBeenCalled()
	})

	it('processes valid payload and acks', async () => {
		await handler.handlePaymentRequested(validPayload, context)

		expect(paymentService.processPaymentRequested).toHaveBeenCalledWith(validPayload)
		expect(idempotency.isDuplicate(validPayload.orderNumber)).toBe(true)
		expect(ack).toHaveBeenCalled()
	})

	it('fail-fast: stripe error publishes payment_failed then acks', async () => {
		paymentService.processPaymentRequested.mockRejectedValue(new Error('stripe down'))

		await handler.handlePaymentRequested(validPayload, context)

		expect(paymentService.publishPaymentFailed).toHaveBeenCalledWith(
			validPayload.orderNumber,
			'stripe_error',
		)
		expect(ack).toHaveBeenCalled()
	})

	it('nacks on unexpected error after parse', async () => {
		paymentService.processPaymentRequested.mockImplementation(() => {
			idempotency.markProcessed = () => {
				throw new Error('unexpected')
			}
			return Promise.resolve()
		})

		await handler.handlePaymentRequested(validPayload, context)

		expect(nack).toHaveBeenCalled()
	})
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run from `services/payment/`:

```bash
pnpm test -- payment-consumer.handler.spec.ts
```

Expected: FAIL — handler file missing.

- [ ] **Step 3: Implement handler**

Remove CLI-generated `payment-consumer.controller.ts` if present.

Create `services/payment/src/payment-consumer/payment-consumer.handler.ts`:

```typescript
import { PaymentRequestedSchema, ROUTING_KEYS } from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices'
import { IdempotencyStore } from '../common/idempotency.store'
import { PaymentService } from '../payment/payment.service'

@Controller()
export class PaymentConsumerHandler {
	private readonly logger = new Logger(PaymentConsumerHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly paymentService: PaymentService,
	) {}

	@EventPattern(ROUTING_KEYS.PAYMENT_REQUESTED)
	async handlePaymentRequested(
		@Payload() payload: unknown,
		@Ctx() context: RmqContext,
	) {
		const channel = context.getChannelRef()
		const msg = context.getMessage()

		try {
			const data = PaymentRequestedSchema.parse(payload)

			if (this.idempotency.isDuplicate(data.orderNumber)) {
				this.logger.warn(
					`Duplicate orders.payment.requested: ${data.orderNumber}`,
				)
				channel.ack(msg)
				return
			}

			this.logger.log(`Processing payment for order ${data.orderNumber}`)

			try {
				await this.paymentService.processPaymentRequested(data)
			} catch (err) {
				this.logger.error('Stripe call failed', err)
				await this.paymentService.publishPaymentFailed(
					data.orderNumber,
					'stripe_error',
				)
			}

			this.idempotency.markProcessed(data.orderNumber)
			channel.ack(msg)
		} catch (err) {
			this.logger.error('Failed to process orders.payment.requested', err)
			channel.nack(msg, false, false)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run from `services/payment/`:

```bash
pnpm test -- payment-consumer.handler.spec.ts
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/payment/src/payment-consumer
git commit -m "feat(payment): add RabbitMQ consumer handler with tests"
```

---

### Task 7: WebhooksController — TDD (with symmetric dedup)

**Files:**
- Modify: `services/payment/src/webhooks/webhooks.controller.ts`
- Create: `services/payment/src/webhooks/webhooks.controller.spec.ts`

- [ ] **Step 1: Write failing controller tests**

Create `services/payment/src/webhooks/webhooks.controller.spec.ts`:

```typescript
import {
	ArgumentsHost,
	Catch,
	ExceptionFilter,
	HttpStatus,
} from '@nestjs/common'
import { FastifyReply } from 'fastify'
import { Test, TestingModule } from '@nestjs/testing'
import { ZodError } from 'zod'
import { IdempotencyStore } from '../common/idempotency.store'
import { PaymentService } from '../payment/payment.service'
import { WebhooksController } from './webhooks.controller'

@Catch(ZodError)
class TestZodFilter implements ExceptionFilter {
	catch(error: ZodError, host: ArgumentsHost) {
		const response = host.switchToHttp().getResponse<FastifyReply>()
		response.status(HttpStatus.BAD_REQUEST).send({
			statusCode: HttpStatus.BAD_REQUEST,
			error: 'Bad Request',
		})
	}
}

describe('WebhooksController', () => {
	let controller: WebhooksController
	let paymentService: jest.Mocked<Pick<PaymentService, 'handleStripeWebhook'>>
	let idempotency: IdempotencyStore

	const validBody = {
		type: 'payment_intent.succeeded' as const,
		data: {
			orderNumber: '22222222-2222-4222-8222-222222222222',
			amount: 99.9,
			reserveId: '11111111-1111-4111-8111-111111111111',
			customerEmail: 'buyer@example.com',
		},
	}

	beforeEach(async () => {
		idempotency = new IdempotencyStore()
		paymentService = {
			handleStripeWebhook: jest.fn().mockResolvedValue(undefined),
		}

		const module: TestingModule = await Test.createTestingModule({
			controllers: [WebhooksController],
			providers: [
				{ provide: IdempotencyStore, useValue: idempotency },
				{ provide: PaymentService, useValue: paymentService },
			],
		}).compile()

		controller = module.get(WebhooksController)
	})

	it('delegates valid webhook to PaymentService', async () => {
		const result = await controller.stripeWebhook(validBody)

		expect(paymentService.handleStripeWebhook).toHaveBeenCalled()
		expect(result).toEqual({ received: true })
	})

	it('throws ZodError on invalid body', async () => {
		await expect(controller.stripeWebhook({ invalid: true })).rejects.toBeInstanceOf(
			ZodError,
		)
	})

	it('skips duplicate webhook using orderNumber:type key', async () => {
		const key = `${validBody.data.orderNumber}:${validBody.type}`
		idempotency.markProcessed(key)

		const result = await controller.stripeWebhook(validBody)

		expect(result).toEqual({ received: true })
		expect(paymentService.handleStripeWebhook).not.toHaveBeenCalled()
	})
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run from `services/payment/`:

```bash
pnpm test -- webhooks.controller.spec.ts
```

Expected: FAIL — controller still default scaffold.

- [ ] **Step 3: Implement WebhooksController with dedup**

Replace `services/payment/src/webhooks/webhooks.controller.ts`:

```typescript
import { StripeWebhookSchema } from '@eda/contracts'
import { Body, Controller, Post } from '@nestjs/common'
import { IdempotencyStore } from '../common/idempotency.store'
import { PaymentService } from '../payment/payment.service'

@Controller('webhooks')
export class WebhooksController {
	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly paymentService: PaymentService,
	) {}

	@Post('stripe')
	async stripeWebhook(@Body() body: unknown) {
		const event = StripeWebhookSchema.parse(body)
		const idempotencyKey = `${event.data.orderNumber}:${event.type}`

		if (this.idempotency.isDuplicate(idempotencyKey)) {
			return { received: true }
		}

		await this.paymentService.handleStripeWebhook(event)
		this.idempotency.markProcessed(idempotencyKey)
		return { received: true }
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run from `services/payment/`:

```bash
pnpm test -- webhooks.controller.spec.ts
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/payment/src/webhooks
git commit -m "feat(payment): add Stripe webhook controller with symmetric dedup"
```

---

### Task 8: Wire modules and hybrid bootstrap

**Files:**
- Modify: `services/payment/src/payment/payment.module.ts`
- Create: `services/payment/src/payment-inbound/payment-inbound.module.ts`
- Modify: `services/payment/src/app.module.ts`
- Modify: `services/payment/src/main.ts`
- Delete: default scaffold files not needed (`app.controller.ts`, `app.service.ts`, `app.controller.spec.ts` if unused)

- [ ] **Step 1: Wire PaymentModule**

Replace `services/payment/src/payment/payment.module.ts`:

```typescript
import { EXCHANGES } from '@eda/contracts'
import { Module } from '@nestjs/common'
import { ClientsModule, Transport } from '@nestjs/microservices'
import { IdempotencyStore } from '../common/idempotency.store'
import { requireEnv } from '../common/env'
import { PAYMENT_GATEWAY } from '../gateways/payment.gateway'
import { StripePaymentGateway } from '../gateways/stripe-payment.gateway'
import { DOMAIN_EVENT_PUBLISHER } from '../messaging/domain-event.publisher'
import { KafkaDomainEventPublisher } from '../messaging/kafka-domain-event.publisher'
import { PaymentService } from './payment.service'

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
	exports: [PaymentService, IdempotencyStore],
})
export class PaymentModule {}
```

- [ ] **Step 2: Create PaymentInboundModule**

Create `services/payment/src/payment-inbound/payment-inbound.module.ts`:

```typescript
import { Module } from '@nestjs/common'
import { PaymentConsumerHandler } from '../payment-consumer/payment-consumer.handler'
import { PaymentModule } from '../payment/payment.module'
import { WebhooksController } from '../webhooks/webhooks.controller'

@Module({
	imports: [PaymentModule],
	controllers: [PaymentConsumerHandler, WebhooksController],
})
export class PaymentInboundModule {}
```

- [ ] **Step 3: Wire AppModule**

Replace `services/payment/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common'
import { HealthController } from './common/health.controller'
import { PaymentInboundModule } from './payment-inbound/payment-inbound.module'
import { PaymentModule } from './payment/payment.module'

@Module({
	imports: [PaymentModule, PaymentInboundModule],
	controllers: [HealthController],
})
export class AppModule {}
```

- [ ] **Step 4: Replace main.ts hybrid bootstrap**

Replace `services/payment/src/main.ts`:

```typescript
import 'dotenv/config'
import { NestFactory } from '@nestjs/core'
import { MicroserviceOptions, Transport } from '@nestjs/microservices'
import {
	FastifyAdapter,
	NestFastifyApplication,
} from '@nestjs/platform-fastify'
import { EXCHANGES } from '@eda/contracts'
import { AppModule } from './app.module'
import { requireEnv } from './common/env'
import { ZodValidationExceptionFilter } from './common/zod-validation.filter'

async function bootstrap() {
	const app = await NestFactory.create<NestFastifyApplication>(
		AppModule,
		new FastifyAdapter(),
	)

	app.useGlobalFilters(new ZodValidationExceptionFilter())

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
	})

	await app.startAllMicroservices()
	const port = Number(process.env.PORT ?? 3010)
	await app.listen(port, '0.0.0.0')
	console.log(`payment-service listening on ${port}`)
}

bootstrap().catch((err) => {
	console.error(err)
	process.exit(1)
})
```

- [ ] **Step 5: Remove unused default scaffold (optional cleanup)**

Delete if present and unused:
- `services/payment/src/app.controller.ts`
- `services/payment/src/app.service.ts`
- `services/payment/src/app.controller.spec.ts`

Remove their references from any module imports.

- [ ] **Step 6: Build**

Run from `services/payment/`:

```bash
pnpm build
```

Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
git add services/payment/src
git commit -m "feat(payment): wire modules and hybrid RabbitMQ + HTTP bootstrap"
```

---

### Task 9: E2E tests — HTTP webhook (mocked infra)

**Files:**
- Create: `services/payment/test/payment.e2e-spec.ts`

- [ ] **Step 1: Write e2e tests**

Create `services/payment/test/payment.e2e-spec.ts`:

```typescript
import { INestApplication } from '@nestjs/common'
import { Test, TestingModule } from '@nestjs/testing'
import {
	FastifyAdapter,
	NestFastifyApplication,
} from '@nestjs/platform-fastify'
import request from 'supertest'
import { HealthController } from '../src/common/health.controller'
import { ZodValidationExceptionFilter } from '../src/common/zod-validation.filter'
import {
	DOMAIN_EVENT_PUBLISHER,
	DomainEventPublisher,
} from '../src/messaging/domain-event.publisher'
import { PaymentInboundModule } from '../src/payment-inbound/payment-inbound.module'
import { PaymentModule } from '../src/payment/payment.module'
import { PAYMENT_GATEWAY, PaymentGateway } from '../src/gateways/payment.gateway'

describe('Payment Service (e2e)', () => {
	let app: INestApplication
	const mockPublisher: jest.Mocked<DomainEventPublisher> = {
		publishPaymentSucceeded: jest.fn().mockResolvedValue(undefined),
		publishPaymentFailed: jest.fn().mockResolvedValue(undefined),
	}
	const mockGateway: jest.Mocked<PaymentGateway> = {
		createPaymentIntent: jest.fn().mockResolvedValue(undefined),
	}

	const validWebhook = {
		type: 'payment_intent.succeeded',
		data: {
			orderNumber: '22222222-2222-4222-8222-222222222222',
			amount: 99.9,
			reserveId: '11111111-1111-4111-8111-111111111111',
			customerEmail: 'buyer@example.com',
		},
	}

	beforeEach(async () => {
		const moduleFixture: TestingModule = await Test.createTestingModule({
			imports: [PaymentModule, PaymentInboundModule],
			controllers: [HealthController],
		})
			.overrideProvider(DOMAIN_EVENT_PUBLISHER)
			.useValue(mockPublisher)
			.overrideProvider(PAYMENT_GATEWAY)
			.useValue(mockGateway)
			.compile()

		app = moduleFixture.createNestApplication<NestFastifyApplication>(
			new FastifyAdapter(),
		)
		app.useGlobalFilters(new ZodValidationExceptionFilter())
		await app.init()
		await app.getHttpAdapter().getInstance().ready()
	})

	afterEach(async () => {
		await app.close()
		jest.clearAllMocks()
	})

	it('GET /health returns ok', () => {
		return request(app.getHttpServer()).get('/health').expect(200).expect({
			status: 'ok',
		})
	})

	it('POST /webhooks/stripe valid payload returns received true', async () => {
		await request(app.getHttpServer())
			.post('/webhooks/stripe')
			.send(validWebhook)
			.expect(200)
			.expect({ received: true })

		expect(mockPublisher.publishPaymentSucceeded).toHaveBeenCalled()
	})

	it('POST /webhooks/stripe invalid payload returns 400', () => {
		return request(app.getHttpServer())
			.post('/webhooks/stripe')
			.send({ invalid: true })
			.expect(400)
	})
})
```

- [ ] **Step 2: Run e2e tests**

Run from `services/payment/`:

```bash
pnpm test:e2e
```

Expected: 3 tests PASS. No RabbitMQ/Kafka/Docker required — providers are overridden.

- [ ] **Step 3: Run full test suite**

Run from `services/payment/`:

```bash
pnpm test && pnpm test:e2e
```

Expected: all unit + e2e tests PASS.

- [ ] **Step 4: Commit**

```bash
git add services/payment/test/payment.e2e-spec.ts
git commit -m "test(payment): add e2e webhook tests with mocked infra"
```

---

### Task 10: Lint, final verification, manual smoke

**Files:**
- Modify: `services/payment/.gitignore` (ensure `.env` ignored — Nest default usually includes it)

- [ ] **Step 1: Lint payment service**

Run from `services/payment/`:

```bash
pnpm lint
```

Expected: no errors. If formatting issues:

```bash
pnpm lint:fix
```

- [ ] **Step 2: Final build**

Run from `services/payment/`:

```bash
pnpm build
```

Expected: build succeeds.

- [ ] **Step 3: Manual smoke (optional, requires infra running)**

With Docker brokers and stripe-mock running:

```bash
# terminal 1 — from services/payment/
pnpm start:dev

# terminal 2 — stripe-mock (if not already running)
PAYMENT_WEBHOOK_URL=http://localhost:3010/webhooks/stripe PORT=3001 pnpm --filter @eda/stripe-mock start

# terminal 3 — create order via api-gateway
curl -s -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -d '{"productId":"prod-1","amount":49.99,"customerEmail":"test@example.com"}'
```

Expected in payment-service logs:
- `Processing payment for order <uuid>`
- stripe-mock delivers webhook
- Kafka event published (verify via api-gateway SSE or logs)

- [ ] **Step 4: Final commit (if lint fixes)**

```bash
git add services/payment
git commit -m "feat: add payment service with zod validation, gateway, and event publisher ports"
```

---

## Spec coverage checklist (self-review)

| Spec requirement | Task |
|------------------|------|
| `StripeWebhook` contract | Task 1 |
| Standalone Nest project (api-gateway pattern) | Task 2 |
| `PaymentGateway` + `StripePaymentGateway` | Task 3 |
| `DomainEventPublisher` + `KafkaDomainEventPublisher` | Task 4 |
| `PaymentService` orchestration | Task 5 |
| RabbitMQ handler + fail-fast + dedup | Task 6 |
| Webhook controller + symmetric dedup | Task 7 |
| `PaymentModule` + `PaymentInboundModule` | Task 8 |
| Hybrid bootstrap (RMQ + Fastify) | Task 8 |
| Unit tests PaymentService | Task 5 |
| Handler/controller tests | Tasks 6–7 |
| E2E webhook (mocked infra) | Task 9 |
| Env vars / port 3010 | Task 2 |
| Documented improvements (not implemented) | N/A — see spec §9 |

No placeholders. All code paths defined.

---

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-06-24-payment-service.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach do you prefer?
