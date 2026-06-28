import { StripeWebhookSchema } from '@eda/contracts'
import { Body, Controller, Post } from '@nestjs/common'
import { PaymentService } from 'src/payment/payment.service'

@Controller('webhooks')
export class WebhooksController {
	constructor(private readonly paymentService: PaymentService) {}

	@Post('stripe')
	async stripeWebhook(@Body() body: unknown) {
		const event = StripeWebhookSchema.parse(body)

		await this.paymentService.handleStripeWebhook(event)

		return { received: true }
	}
}
