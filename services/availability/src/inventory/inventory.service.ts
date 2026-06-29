import { Inject, Injectable } from '@nestjs/common'
import {
	INVENTORY_REPOSITORY,
	InventoryRepository,
} from './inventory.repository'

@Injectable()
export class InventoryService {
	constructor(
		@Inject(INVENTORY_REPOSITORY)
		private readonly inventoryRepository: InventoryRepository,
	) {}

	confirmReservation(reserveId: string): void {
		this.inventoryRepository.confirmReservation(reserveId)
	}

	releaseReservation(reserveId: string): void {
		this.inventoryRepository.releaseReservation(reserveId)
	}
}
