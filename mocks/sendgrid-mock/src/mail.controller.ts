import { Body, Controller, HttpCode, Post } from '@nestjs/common'
import { MailService } from './mail.service'

@Controller('v3/mail')
export class MailController {
	constructor(private readonly mailService: MailService) {}

	@Post('send')
	@HttpCode(202)
	send(
		@Body() body: {
			personalizations: Array<{ to: Array<{ email: string }> }>
			subject: string
		},
	) {
		return this.mailService.send(body)
	}
}
