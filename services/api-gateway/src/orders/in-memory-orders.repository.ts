import { Injectable } from '@nestjs/common'
import { Order, OrderStatus } from './order.entity'
import { OrdersRepository } from './orders.repository'

@Injectable()
export class InMemoryOrdersRepository implements OrdersRepository {
	private readonly orders = new Map<string, Order>()

	save(order: Order): void {
		this.orders.set(order.orderNumber, order)
	}

	findByOrderNumber(orderNumber: string): Order | null {
		return this.orders.get(orderNumber) ?? null
	}

	updateStatus(orderNumber: string, status: OrderStatus): Order | null {
		const order = this.orders.get(orderNumber)

		if (!order) return null

		order.status = status
		return order
	}
}
