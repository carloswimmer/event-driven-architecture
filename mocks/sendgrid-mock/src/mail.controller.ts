import { Body, Controller, HttpCode, Post } from '@nestjs/common'
import { MailService } from './mail.service'
import { SendMailRequestDto } from './mail.types'

@Controller('v3/mail')
export class MailController {
	constructor(private readonly mailService: MailService) {}

	@Post('send')
	@HttpCode(202)
	send(@Body() body: SendMailRequestDto) {
		return this.mailService.sendMail(body)
	}
}
