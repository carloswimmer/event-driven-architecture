import { Module } from '@nestjs/common'
import { HealthController } from './common/health.controller'
import { InvoiceModule } from './invoice/invoice.module'

@Module({
	imports: [InvoiceModule],
	controllers: [HealthController],
})
export class AppModule {}
