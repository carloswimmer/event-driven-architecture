import { EXCHANGES } from '@eda/contracts'
import { Module } from '@nestjs/common'
import { ClientsModule, Transport } from '@nestjs/microservices'
import { requireEnv } from 'src/common/env'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { EMAIL_GATEWAY } from 'src/gateways/email.gateway'
import { SendGridEmailGateway } from 'src/gateways/sendgrid-email.gateway'
import { InvoiceEventsHandler } from 'src/invoice-events/invoice-events.handler'
import { ANALYTICS_COMMAND_PUBLISHER } from 'src/messaging/analytics-command.publisher'
import { RabbitMqAnalyticsCommandPublisher } from 'src/messaging/rabbitmq-analytics-command.publisher'
import { WebhooksController } from 'src/webhooks/webhooks.controller'
import { NotificationService } from './notification.service'

const rabbitMqRegister = ClientsModule.register([
	{
		name: 'RABBITMQ_EMAIL_DELIVERED',
		transport: Transport.RMQ,
		options: {
			urls: [requireEnv('RABBITMQ_URL')],
			queue: 'notifications.email.delivered',
			noAssert: true,
			queueOptions: { durable: true },
			wildcards: true,
			exchange: EXCHANGES.COMMANDS,
			exchangeType: 'topic',
		},
	},
	{
		name: 'RABBITMQ_EMAIL_FAILED',
		transport: Transport.RMQ,
		options: {
			urls: [requireEnv('RABBITMQ_URL')],
			queue: 'notifications.email.not.delivered',
			noAssert: true,
			queueOptions: { durable: true },
			wildcards: true,
			exchange: EXCHANGES.COMMANDS,
			exchangeType: 'topic',
		},
	},
])

@Module({
	imports: [rabbitMqRegister],
	controllers: [InvoiceEventsHandler, WebhooksController],
	providers: [
		IdempotencyStore,
		NotificationService,
		{ provide: EMAIL_GATEWAY, useClass: SendGridEmailGateway },
		{
			provide: ANALYTICS_COMMAND_PUBLISHER,
			useClass: RabbitMqAnalyticsCommandPublisher,
		},
	],
})
export class NotificationModule {}
