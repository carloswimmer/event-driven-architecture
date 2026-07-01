import {
	EmailDeliveredSchema,
	EmailFailedSchema,
	InvoiceCreated,
	SendGridWebhookEvent,
	SendGridWebhookEventSchema,
} from '@eda/contracts'
import { Inject, Injectable, Logger } from '@nestjs/common'
import { EMAIL_GATEWAY, EmailGateway } from 'src/gateways/email.gateway'
import {
	ANALYTICS_COMMAND_PUBLISHER,
	AnalyticsCommandPublisher,
} from 'src/messaging/analytics-command.publisher'

@Injectable()
export class NotificationService {
	private readonly logger = new Logger(NotificationService.name)
	constructor(
		@Inject(EMAIL_GATEWAY) private readonly emailGateway: EmailGateway,
		@Inject(ANALYTICS_COMMAND_PUBLISHER)
		private readonly analyticsCommands: AnalyticsCommandPublisher,
	) {}

	async sendInvoiceNotification({
		customerInfo,
		invoiceId,
		orderNumber,
	}: InvoiceCreated) {
		await this.emailGateway.sendInvoiceEmail(
			customerInfo.email,
			invoiceId,
			orderNumber,
		)
	}

	async handleSendGridEvent(raw: SendGridWebhookEvent) {
		const { event, email, invoiceId, orderNumber, sg_message_id, reason } =
			SendGridWebhookEventSchema.parse(raw)

		if (event === 'processed') {
			this.logger.log(`SendGrid processed: ${email}`)
			return
		}

		if (!invoiceId || !orderNumber) {
			throw new Error('SendGrid webhook is missing invoiceId or orderNumber')
		}

		if (event === 'delivered') {
			const payload = EmailDeliveredSchema.parse({
				invoiceId,
				orderNumber,
				email,
				sgMessageId: sg_message_id,
			})

			this.logger.log(`SendGrid delivered: ${email}`)
			await this.analyticsCommands.publishEmailDelivered(payload)

			return
		}

		if (event === 'bounce') {
			const payload = EmailFailedSchema.parse({
				invoiceId,
				orderNumber,
				email,
				reason: reason ?? event,
			})

			this.logger.log(`SendGrid bounce: ${email}`)
			await this.analyticsCommands.publishEmailFailed(payload)
		}
	}
}
