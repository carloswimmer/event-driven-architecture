import { randomUUID } from 'node:crypto'
import { Injectable, Logger } from '@nestjs/common'
import { requireEnv } from 'packages/shared/dist'
import { SendMailRequestDto } from './mail.types'

@Injectable()
export class MailService {
	private readonly logger = new Logger(MailService.name)
	private readonly webhookUrl = requireEnv('NOTIFICATION_WEBHOOK_URL')

	sendMail(body: SendMailRequestDto): { message: string } {
		const personalization = body.personalizations?.[0]
		const email = personalization?.to?.[0]?.email ?? 'unknown'
		const customArgs = personalization?.custom_args ?? {}
		const sgMessageId = `sg_mock_${randomUUID()}`

		this.logger.log(`Accepted email to ${email}: ${body.subject}`)

		setTimeout(() => {
			void this.emitEvents(email, sgMessageId, customArgs)
		}, 2000)

		return { message: 'accepted' }
	}

	private async emitEvents(
		email: string,
		sgMessageId: string,
		customArgs: Record<string, string>,
	): Promise<void> {
		// to test send mail failed, we use the strategy of 'email ready to fail' (bounce@...)
		const shouldBounce = email.includes('bounce@')

		const base = {
			email,
			timestamp: Math.floor(Date.now() / 1000),
			sg_message_id: sgMessageId,
			...customArgs,
		}

		await this.postWebhook([{ ...base, event: 'processed' }])

		if (shouldBounce) {
			await this.postWebhook([
				{
					...base,
					event: 'bounce',
					reason: '500 unknown recipient',
					type: 'bounce',
				},
			])
			return
		}

		await this.postWebhook([
			{ ...base, event: 'delivered', response: '250 OK' },
		])
	}

	private async postWebhook(events: unknown[]) {
		const response = await fetch(this.webhookUrl, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify(events),
		})

		if (!response.ok) {
			this.logger.error(`SendGrid webhook failed: HTTP ${response.status}`)
		}
	}
}
