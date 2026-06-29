import {
	InvoiceCreatedSchema,
	PaymentFailedSchema,
	PaymentSucceededSchema,
	TOPICS,
} from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { EventPattern, Payload } from '@nestjs/microservices'
import { EventsService } from 'src/events/events.service'

@Controller('kafka-events')
export class KafkaEventsHandler {
	private readonly logger = new Logger(KafkaEventsHandler.name)

	constructor(private readonly eventsService: EventsService) {}

	@EventPattern(TOPICS.PAYMENT_SUCCEEDED)
	handlePaymentSucceeded(@Payload() payload: unknown) {
		const event = PaymentSucceededSchema.parse(payload)
		this.logger.log(`Recorded orders.payment.succeeded ${event.orderNumber}`)
		this.eventsService.record('orders.payment.succeeded', event)
	}

	@EventPattern(TOPICS.PAYMENT_FAILED)
	handlePaymentFailed(@Payload() payload: unknown) {
		const event = PaymentFailedSchema.parse(payload)
		this.logger.log(`Recorded orders.payment.failed ${event.orderNumber}`)
		this.eventsService.record('orders.payment.failed', event)
	}

	@EventPattern(TOPICS.INVOICE_CREATED)
	handleInvoiceCrated(@Payload() payload: unknown) {
		const event = InvoiceCreatedSchema.parse(payload)
		this.logger.log(`Recorded billing.invoice.created: ${event.invoiceId}`)
		this.eventsService.record('billing.invoice.created', event)
	}
}
