import { Logger } from '@nestjs/common'
import { InventoryRepository } from './inventory.repository'

export class InMemoryInventoryRepository implements InventoryRepository {
	private readonly logger = new Logger(InMemoryInventoryRepository.name)
	private stock = 100

	confirmReservation(reserveId: string): void {
		this.stock -= 1
		this.logger.log(
			`Confirmed reservation ${reserveId}. Remaining stock: ${this.stock}`,
		)
	}

	releaseReservation(reserveId: string): void {
		this.stock += 1
		this.logger.log(
			`Released reservation ${reserveId}. Remaining stock: ${this.stock}`,
		)
	}
}
