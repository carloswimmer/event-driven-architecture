import { Order, OrderStatus } from './order.entity'

export const ORDERS_REPOSITORY = Symbol('ORDERS_REPOSITORY')

export interface OrdersRepository {
	save(order: Order): void
	findByOrderNumber(orderNumber: string): Order | null
	updateStatus(orderNumber: string, status: OrderStatus): Order | null
}
