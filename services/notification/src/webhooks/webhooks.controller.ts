import { SendGridWebhookPayloadSchema } from '@eda/contracts'
import { Body, Controller, HttpCode, Post } from '@nestjs/common'

@Controller('webhooks')
export class WebhooksController {
	@Post('sendgrid')
	@HttpCode(204)
	async sendgrid(@Body() body: unknown) {
		const event = SendGridWebhookPayloadSchema.parse(body)
	}
}
