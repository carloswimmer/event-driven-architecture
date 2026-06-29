export const INVENTORY_REPOSITORY = Symbol('INVENTORY_REPOSITORY')

export interface InventoryRepository {
	confirmReservation(reserveId: string): void
	releaseReservation(reserveId: string): void
}
