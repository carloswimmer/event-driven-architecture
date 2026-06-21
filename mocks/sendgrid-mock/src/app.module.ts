import { HealthController } from '@eda/shared'
import { Module } from '@nestjs/common'
import { MailController } from './mail.controller'
import { MailService } from './mail.service'

@Module({
	controllers: [HealthController, MailController],
	providers: [MailService],
})
export class AppModule {}
