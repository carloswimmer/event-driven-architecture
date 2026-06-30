import { Module } from '@nestjs/common'
import { HealthController } from './common/health.controller'
import { NotificationModule } from './notification/notification.module'

@Module({
	imports: [NotificationModule],
	controllers: [HealthController],
})
export class AppModule {}
