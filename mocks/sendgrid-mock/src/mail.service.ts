import { Injectable, Logger } from '@nestjs/common'

@Injectable()
export class MailService {
	private readonly logger = new Logger(MailService.name)

	send(body: {
		personalizations: Array<{ to: Array<{ email: string }> }>
		subject: string
	}) {
		const recipient = body.personalizations?.[0]?.to?.[0]?.email ?? 'unknown'
		this.logger.log(`Email sent to ${recipient}: ${body.subject}`)
		return { message: 'accepted' }
	}
}
