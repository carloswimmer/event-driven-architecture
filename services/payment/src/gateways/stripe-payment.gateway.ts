import { PaymentRequested } from '@eda/contracts'
import { Injectable } from '@nestjs/common'
import { requireEnv } from 'src/common/env'
import { PaymentGateway } from './payment.gateway'

@Injectable()
export class StripePaymentGateway implements PaymentGateway {
	private readonly baseUrl = requireEnv('STRIPE_MOCK_URL')

	async createPaymentIntent({
		amount,
		customerEmail,
		orderNumber,
		reserveId,
	}: PaymentRequested): Promise<void> {
		const response = await fetch(`${this.baseUrl}/v1/payment-intents`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ amount, customerEmail, orderNumber, reserveId }),
		})

		if (!response.ok) {
			throw new Error(`🚫 Stripe mock returned HTTP ${response.status}`)
		}
	}
}
