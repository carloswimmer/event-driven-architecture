import {
	PaymentFailedSchema,
	PaymentRequested,
	PaymentSucceededSchema,
	StripeWebhook,
} from '@eda/contracts'
import { Inject, Injectable } from '@nestjs/common'
import { PAYMENT_GATEWAY, PaymentGateway } from 'src/gateways/payment.gateway'
import {
	DOMAIN_EVENT_PUBLISHER,
	DomainEventPublisher,
} from 'src/messaging/domain-event.publisher'

@Injectable()
export class PaymentService {
	constructor(
		@Inject(PAYMENT_GATEWAY) private readonly paymentGateway: PaymentGateway,
		@Inject(DOMAIN_EVENT_PUBLISHER)
		private readonly events: DomainEventPublisher,
	) {}

	public async processPaymentRequested(data: PaymentRequested): Promise<void> {
		await this.paymentGateway.createPaymentIntent(data)
	}

	public async publishPaymentFailed(
		orderNumber: string,
		reserveId: string,
		reason: string,
	): Promise<void> {
		await this.events.publishPaymentFailed({ orderNumber, reserveId, reason })
	}

	public async handleStripeWebhook({
		data,
		type,
	}: StripeWebhook): Promise<void> {
		if (type === 'payment_intent.succeeded') {
			const { amount, customerInfo, orderNumber, reserveId } =
				PaymentSucceededSchema.parse({
					...data,
					customerInfo: { email: data.customerEmail },
				})

			await this.events.publishPaymentSucceeded({
				customerInfo,
				orderNumber,
				reserveId,
				amount,
			})
		}

		if (type === 'payment_intent.payment_failed') {
			const payload = PaymentFailedSchema.parse({
				...data,
				reason: 'payment_intent.payment_failed',
			})
			await this.events.publishPaymentFailed(payload)
		}
	}
}
