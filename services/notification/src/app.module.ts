import { Module } from '@nestjs/common'
import { HealthController } from './common/health.controller'
import { NotificationModule } from './notification/notification.module'
import { WebhooksController } from './webhooks/webhooks.controller';

@Module({
	imports: [NotificationModule],
	controllers: [HealthController, WebhooksController],
})
export class AppModule {}
