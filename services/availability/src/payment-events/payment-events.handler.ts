import { Controller, Logger } from '@nestjs/common'
import { EventPattern, Payload } from '@nestjs/microservices'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { InventoryService } from 'src/inventory/inventory.service'
import {
	PaymentFailedSchema,
	PaymentSucceededSchema,
	TOPICS,
} from '../../../../packages/contracts/dist'

@Controller('payment-events')
export class PaymentEventsHandler {
	private readonly logger = new Logger(PaymentEventsHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly inventoryService: InventoryService,
	) {}

	@EventPattern(TOPICS.PAYMENT_SUCCEEDED)
	handlePaymentSucceeded(@Payload() payload: unknown) {
		const event = PaymentSucceededSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.orderNumber)) {
			this.logger.warn(
				`Duplicate orders.payment.succeeded: ${event.orderNumber}`,
			)
			return
		}

		this.inventoryService.confirmReservation(event.reserveId)
		this.idempotency.markProcessed(event.orderNumber)
	}

	@EventPattern(TOPICS.PAYMENT_FAILED)
	handlePaymentFailed(@Payload() payload: unknown) {
		const event = PaymentFailedSchema.parse(payload)

		if (this.idempotency.isDuplicate(event.orderNumber)) {
			this.logger.warn(`Duplicate orders.payment.failed: ${event.orderNumber}`)
		}

		this.inventoryService.releaseReservation(event.reserveId)
		this.idempotency.markProcessed(event.orderNumber)
	}
}
