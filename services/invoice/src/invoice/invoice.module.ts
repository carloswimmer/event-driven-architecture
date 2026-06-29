import { Module } from '@nestjs/common'
import {
	ClientProviderOptions,
	ClientsModule,
	Transport,
} from '@nestjs/microservices'
import { requireEnv } from 'src/common/env'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { DOMAIN_EVENT_PUBLISHER } from 'src/messaging/domain-event.publisher'
import { KafkaDomainEventPublisher } from 'src/messaging/kafka-domain-event.publisher'
import { PaymentEventsHandler } from 'src/payment-events/payment-events.handler'
import { InvoiceService } from './invoice.service'

const kafkaRegister = {
	name: 'KAFKA_SERVICE',
	transport: Transport.KAFKA,
	options: {
		client: {
			clientId: 'invoice-service',
			brokers: requireEnv('KAFKA_BROKERS').split(','),
		},
		producer: { allowAutoTopicCreation: false },
	},
} satisfies ClientProviderOptions

@Module({
	imports: [ClientsModule.register([kafkaRegister])],
	controllers: [PaymentEventsHandler],
	providers: [
		InvoiceService,
		IdempotencyStore,
		{ provide: DOMAIN_EVENT_PUBLISHER, useClass: KafkaDomainEventPublisher },
	],
})
export class InvoiceModule {}
