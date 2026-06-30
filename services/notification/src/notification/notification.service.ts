import { InvoiceCreated } from '@eda/contracts'
import { Inject, Injectable } from '@nestjs/common'
import { EMAIL_GATEWAY, EmailGateway } from 'src/gateways/email.gateway'

@Injectable()
export class NotificationService {
	constructor(
		@Inject(EMAIL_GATEWAY) private readonly emailGateway: EmailGateway,
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
}
