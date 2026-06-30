import { Module } from '@nestjs/common'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { EMAIL_GATEWAY } from 'src/gateways/email.gateway'
import { SendGridEmailGateway } from 'src/gateways/sendgrid-email.gateway'
import { InvoiceEventsHandler } from 'src/invoice-events/invoice-events.handler'
import { NotificationService } from './notification.service'

@Module({
	controllers: [InvoiceEventsHandler],
	providers: [
		IdempotencyStore,
		NotificationService,
		{ provide: EMAIL_GATEWAY, useClass: SendGridEmailGateway },
	],
})
export class NotificationModule {}
