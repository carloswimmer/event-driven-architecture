import {
	PaymentFailedSchema,
	PaymentSucceededSchema,
	TOPICS,
} from '@eda/contracts'
import { Controller, Logger } from '@nestjs/common'
import { EventPattern, Payload } from '@nestjs/microservices'
import { OrdersService } from 'src/orders/orders.service'

@Controller()
export class PaymentEventsHandler {
	private readonly logger = new Logger(PaymentEventsHandler.name)

	constructor(private readonly ordersService: OrdersService) {}

	@EventPattern(TOPICS.PAYMENT_SUCCEEDED)
	handlePaymentSucceeded(@Payload() payload: unknown) {
		const event = PaymentSucceededSchema.parse(payload)
		this.logger.log(`Payment succeeded for order ${event.orderNumber}`)
		this.ordersService.applyPaymentResult(
			event.orderNumber,
			'payment_succeeded',
		)
	}

	@EventPattern(TOPICS.PAYMENT_FAILED)
	handlePaymentFailed(@Payload() payload: unknown) {
		const event = PaymentFailedSchema.parse(payload)
		this.logger.log(`Payment failed for order ${event.orderNumber}`)
		this.ordersService.applyPaymentResult(event.orderNumber, 'payment_failed')
	}
}
