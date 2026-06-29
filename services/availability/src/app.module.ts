import { Module } from '@nestjs/common'
import { AvailabilityModule } from './availability/availability.module'
import { HealthController } from './common/health.controller'

@Module({
	imports: [AvailabilityModule],
	controllers: [HealthController],
})
export class AppModule {}
