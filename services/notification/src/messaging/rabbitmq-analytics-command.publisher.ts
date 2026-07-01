import {
	EmailDelivered,
	EmailDeliveredSchema,
	EmailFailed,
	EmailFailedSchema,
	ROUTING_KEYS,
} from '@eda/contracts'
import { Inject } from '@nestjs/common'
import { ClientProxy } from '@nestjs/microservices'
import { firstValueFrom } from 'rxjs'
import { AnalyticsCommandPublisher } from './analytics-command.publisher'

export class RabbitMqAnalyticsCommandPublisher
	implements AnalyticsCommandPublisher
{
	constructor(
		@Inject('RABBITMQ_EMAIL_DELIVERED')
		private readonly deliveredClient: ClientProxy,
		@Inject('RABBITMQ_EMAIL_FAILED') private readonly failedClient: ClientProxy,
	) {}

	async publishEmailDelivered(payload: EmailDelivered): Promise<void> {
		const event = EmailDeliveredSchema.parse(payload)

		await firstValueFrom(
			this.deliveredClient.emit(ROUTING_KEYS.EMAIL_DELIVERED, event),
		)
	}

	async publishEmailFailed(payload: EmailFailed): Promise<void> {
		const event = EmailFailedSchema.parse(payload)

		await firstValueFrom(
			this.failedClient.emit(ROUTING_KEYS.EMAIL_FAILED, event),
		)
	}
}
