import { Module } from '@nestjs/common'
import { IdempotencyStore } from 'src/common/idempotency.store'
import { InMemoryInventoryRepository } from 'src/inventory/in-memory-inventory.repository'
import { INVENTORY_REPOSITORY } from 'src/inventory/inventory.repository'
import { InventoryService } from 'src/inventory/inventory.service'
import { PaymentEventsHandler } from 'src/payment-events/payment-events.handler'

@Module({
	controllers: [PaymentEventsHandler],
	providers: [
		IdempotencyStore,
		InventoryService,
		{ provide: INVENTORY_REPOSITORY, useClass: InMemoryInventoryRepository },
	],
})
export class AvailabilityModule {}
