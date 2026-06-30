import { InvoiceCreatedSchema, TOPICS } from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { EventPattern, Payload } from '@nestjs/microservices'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { NotificationService } from 'src/notification/notification.service'

@Controller('invoice-event')
export class InvoiceEventsHandler {
	private readonly logger = new Logger(InvoiceEventsHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly notificationService: NotificationService,
	) {}

	@EventPattern(TOPICS.INVOICE_CREATED)
	async handleInvoiceCreated(@Payload() payload: unknown) {
		const event = InvoiceCreatedSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.invoiceId)) {
			this.logger.warn(`Duplicate billing.invoice.created: ${event.invoiceId}`)
		}

		await this.notificationService.sendInvoiceNotification(event)
		this.idempotency.markProcessed(event.invoiceId)
	}
}
