import {
	EmailDeliveredSchema,
	EmailFailedSchema,
	ROUTING_KEYS,
} from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import {
	Ctx,
	EventPattern,
	Payload,
	RmqContext,
	Transport,
} from '@nestjs/microservices'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { EventsService } from 'src/events/events.service'

@Controller('rabbitmq-events')
export class RabbitMqEventsHandler {
	private readonly logger = new Logger(RabbitMqEventsHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly eventsService: EventsService,
	) {}

	@EventPattern(ROUTING_KEYS.EMAIL_DELIVERED, Transport.RMQ)
	handleEmailDelivered(
		@Payload() payload: unknown,
		@Ctx() context: RmqContext,
	) {
		const channel = context.getChannelRef()
		const msg = context.getMessage()
		const event = EmailDeliveredSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.orderNumber)) {
			this.logger.warn(
				`Duplicate notifications.email.delivered for invoice ${event.invoiceId} `,
			)
			channel.ack(msg)
			return
		}

		this.logger.log(`Recorded notifications.email.delivered: ${event.email}`)
		this.eventsService.record('notifications.email.delivered', event)
	}

	@EventPattern(ROUTING_KEYS.EMAIL_FAILED, Transport.RMQ)
	handleEmailFailed(@Payload() payload: unknown, @Ctx() context: RmqContext) {
		const channel = context.getChannelRef()
		const msg = context.getMessage()
		const event = EmailFailedSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.orderNumber)) {
			this.logger.warn(
				`Duplicate notifications.email.not.delivered for invoice ${event.invoiceId}`,
			)
			channel.ack(msg)
			return
		}

		this.logger.log(
			`Recorded notifications.email.not.delivered: ${event.email}`,
		)
		this.eventsService.record('notifications.email.not.delivered', event)
	}
}
