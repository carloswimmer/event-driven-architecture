import {
	PaymentRequested,
	PaymentRequestedSchema,
	ROUTING_KEYS,
} from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { PaymentService } from 'src/payment/payment.service'

@Controller()
export class PaymentConsumerHandler {
	private readonly logger = new Logger(PaymentConsumerHandler.name)

	constructor(
		private readonly idempotency: IdempotencyStore,
		private readonly paymentService: PaymentService,
	) {}

	@EventPattern(ROUTING_KEYS.PAYMENT_REQUESTED)
	async handlePaymentRequested(
		@Payload() payload: unknown,
		@Ctx() context: RmqContext,
	) {
		const channel = context.getChannelRef()
		const message = context.getMessage()

		try {
			const data = PaymentRequestedSchema.parse(payload)

			if (this.idempotency.isDuplicate(data.orderNumber)) {
				this.logger.warn(
					`Duplicate orders.payment.requested: ${data.orderNumber}`,
				)
				channel.ack(message)
				return
			}

			this.logger.log(`Processing payment for order ${data.orderNumber}`)

			await this.processOrPublishFailure(data)

			this.idempotency.markProcessed(data.orderNumber)
			channel.ack(message)
		} catch (error) {
			this.logger.error('Failed to process orders.payment.requested', error)
			channel.nack(message, false, false)
		}
	}

	private async processOrPublishFailure(data: PaymentRequested): Promise<void> {
		try {
			await this.paymentService.processPaymentRequested(data)
		} catch (error) {
			this.logger.error('Stripe call failed', error)
			await this.paymentService.publishPaymentFailed(
				data.orderNumber,
				'stripe_error',
			)
		}
	}
}
