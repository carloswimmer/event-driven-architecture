import { randomUUID } from 'node:crypto'
import { InvoiceCreatedSchema, PaymentSucceeded } from '@eda/contracts'
import { Inject, Injectable, Logger } from '@nestjs/common'
import {
	DOMAIN_EVENT_PUBLISHER,
	DomainEventPublisher,
} from 'src/messaging/domain-event.publisher'

@Injectable()
export class InvoiceService {
	private readonly logger = new Logger(InvoiceService.name)

	constructor(
		@Inject(DOMAIN_EVENT_PUBLISHER)
		private readonly events: DomainEventPublisher,
	) {}

	async createFromPayment({
		orderNumber,
		amount,
		customerInfo,
	}: PaymentSucceeded) {
		const invoiceId = randomUUID()
		this.logger.log(`Creating invoice ${invoiceId} for order ${orderNumber}`)

		const payload = InvoiceCreatedSchema.parse({
			invoiceId,
			amount,
			customerInfo,
			orderNumber,
		})

		await this.events.publishInvoiceCreated(payload)
	}
}
