import { SendGridWebhookPayloadSchema } from '@eda/contracts'
import { Body, Controller, HttpCode, Post } from '@nestjs/common'
import { NotificationService } from 'src/notification/notification.service'

@Controller('webhooks')
export class WebhooksController {
	constructor(private readonly notificationService: NotificationService) {}

	@Post('sendgrid')
	@HttpCode(204)
	async sendgrid(@Body() body: unknown) {
		const events = SendGridWebhookPayloadSchema.parse(body)

		for (const event of events) {
			await this.notificationService.handleSendGridEvent(event)
		}
	}
}
