import { Body, Controller, Post } from '@nestjs/common'
import { StripeService } from './stripe.service'
import type { CreatePaymentIntentRequestDto } from './stripe.types'

@Controller('v1')
export class StripeController {
	constructor(private readonly stripeService: StripeService) {}

	@Post('payment-intents')
	createPaymentIntent(@Body() body: CreatePaymentIntentRequestDto) {
		return this.stripeService.createPaymentIntent(body)
	}
}
