import { Module } from '@nestjs/common'
import {
	ClientProviderOptions,
	ClientsModule,
	Transport,
} from '@nestjs/microservices'
import { requireEnv } from 'src/common/env'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { PAYMENT_GATEWAY } from 'src/gateways/payment.gateway'
import { StripePaymentGateway } from 'src/gateways/stripe-payment.gateway'
import { DOMAIN_EVENT_PUBLISHER } from 'src/messaging/domain-event.publisher'
import { KafkaDomainEventPublisher } from 'src/messaging/kafka-domain-event.publisher'
import { PaymentConsumerHandler } from 'src/payment-consumer/payment-consumer.controller'
import { WebhooksController } from 'src/webhooks/webhooks.controller'
import { PaymentService } from './payment.service'

const kafkaRegister = {
	name: 'KAFKA_SERVICE',
	transport: Transport.KAFKA,
	options: {
		client: {
			clientId: 'payment-service',
			brokers: requireEnv('KAFKA_BROKERS').split(','),
		},
		producer: { allowAutoTopicCreation: false },
	},
} satisfies ClientProviderOptions

@Module({
	imports: [ClientsModule.register([kafkaRegister])],
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
