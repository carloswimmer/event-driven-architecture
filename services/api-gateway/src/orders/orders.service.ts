import { randomUUID } from 'node:crypto'
import { Inject, Injectable, NotFoundException } from '@nestjs/common'
import {
	PAYMENT_COMMAND_PUBLISHER,
	PaymentCommandPublisher,
} from 'src/messaging/payment-command.publisher'
import { CreateOrderInput } from './create-order.schema'
import { Order, OrderStatus } from './order.entity'
import { OrderStatusStreamService } from './order-status-stream.service'
import { ORDERS_REPOSITORY, OrdersRepository } from './orders.repository'

@Injectable()
export class OrdersService {
	constructor(
		@Inject(ORDERS_REPOSITORY)
		private readonly ordersRepository: OrdersRepository,
		@Inject(PAYMENT_COMMAND_PUBLISHER)
		private readonly commandPublisher: PaymentCommandPublisher,
		private readonly statusStream: OrderStatusStreamService,
	) {}

	async createOrder(input: CreateOrderInput) {
		const orderNumber = randomUUID()
		const reserveId = randomUUID()

		this.ordersRepository.save({
			orderNumber,
			reserveId,
			productId: input.productId,
			amount: input.amount,
			customerEmail: input.customerEmail,
			status: 'payment_pending',
		})

		this.statusStream.register(orderNumber)

		await this.commandPublisher.publishPaymentRequested({
			reserveId,
			orderNumber,
			amount: input.amount,
			customerEmail: input.customerEmail,
		})

		return { orderNumber, status: 'payment_pending' as const }
	}

	getOrder(orderNumber: string): Order {
		const order = this.ordersRepository.findByOrderNumber(orderNumber)

		if (!order) {
			throw new NotFoundException(`Order ${orderNumber} not found`)
		}

		return order
	}

	applyPaymentResult(
		orderNumber: string,
		status: Extract<OrderStatus, 'payment_succeeded' | 'payment_failed'>,
	): void {
		const order = this.ordersRepository.updateStatus(orderNumber, status)

		if (!order) return

		this.statusStream.push(orderNumber, status)
	}
}
