import { PaymentSucceededSchema, TOPICS } from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { EventPattern, Payload } from '@nestjs/microservices'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { InvoiceService } from 'src/invoice/invoice.service'

@Controller('payment-events')
export class PaymentEventsHandler {
	private readonly logger = new Logger(PaymentEventsHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly invoiceService: InvoiceService,
	) {}

	@EventPattern(TOPICS.PAYMENT_SUCCEEDED)
	async handlePaymentSucceeded(@Payload() payload: unknown) {
		const event = PaymentSucceededSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.orderNumber)) {
			this.logger.warn(
				`Duplicate orders.payment.succeeded ${event.orderNumber}`,
			)
			return
		}

		await this.invoiceService.createFromPayment(event)
		this.idempotency.markProcessed(event.orderNumber)
	}
}
