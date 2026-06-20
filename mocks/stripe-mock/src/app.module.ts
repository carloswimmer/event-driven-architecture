import { HealthController } from '@eda/shared'
import { Module } from '@nestjs/common'
import { StripeController } from './stripe.controller'
import { StripeService } from './stripe.service'

@Module({
	controllers: [HealthController, StripeController],
	providers: [StripeService],
})
export class AppModule {}
