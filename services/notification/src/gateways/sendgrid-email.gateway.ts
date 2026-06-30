import { Injectable, Logger } from '@nestjs/common'
import { requireEnv } from 'src/common/env'
import { EmailGateway } from './email.gateway'

@Injectable()
export class SendGridEmailGateway implements EmailGateway {
	private readonly logger = new Logger(SendGridEmailGateway.name)
	private readonly baseUrl = requireEnv('SENDGRID_MOCK_URL')

	async sendInvoiceEmail(
		email: string,
		invoiceId: string,
		orderNumber: string,
	): Promise<void> {
		const response = await fetch(`${this.baseUrl}/v3/mail/send`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({
				personalizations: [
					{ to: [{ email }], custom_args: { invoiceId, orderNumber } },
				],
				from: { email: 'noreply@eda.local' },
				subject: `Invoice ${invoiceId} for order ${orderNumber}`,
				content: [
					{
						type: 'text/plain',
						value: `Thank you for shopping with us. Here is your invoice number ${invoiceId}.`,
					},
				],
			}),
		})

		if (!response.ok) {
			throw new Error(`SendGrid mock returned HTTP ${response.status}`)
		}

		this.logger.log(`Invoice email sent to ${email}`)
	}
}
