import { randomUUID } from 'node:crypto'
import { requireEnv } from '@eda/shared'
import { Injectable, Logger } from '@nestjs/common'
import type {
	CreatePaymentIntentRequestDto,
	CreatePaymentIntentResponseDto,
} from './stripe.types'

@Injectable()
export class StripeService {
	private readonly logger = new Logger(StripeService.name)
	private readonly webhookUrl = requireEnv('PAYMENT_WEBHOOK_URL')

	createPaymentIntent(
		body: CreatePaymentIntentRequestDto,
	): CreatePaymentIntentResponseDto {
		const intentId = `pi_mock_${randomUUID()}`
		this.logger.log(
			`Created PaymentIntent ${intentId} for order ${body.orderNumber}`,
		)

		setTimeout(() => {
			void this.sendWebhook(body)
		}, 500)

		return { id: intentId, status: 'processing' }
	}

	private async sendWebhook(body: CreatePaymentIntentRequestDto) {
		const payload = {
			type: 'payment_intent.succeeded',
			data: { ...body },
		}

		this.logger.log(`Sending webhook to ${this.webhookUrl}`)

		const response = await fetch(this.webhookUrl, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify(payload),
		})

		if (!response.ok) {
			this.logger.error(`Webhook failed: HTTP ${response.status}`)
		}
	}
}
