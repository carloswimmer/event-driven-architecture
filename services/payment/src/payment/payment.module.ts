import { Module } from '@nestjs/common'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { PaymentService } from './payment.service'

@Module({
	providers: [IdempotencyStore, PaymentService],
})
export class PaymentModule {}
