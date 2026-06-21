import { Injectable, NotFoundException } from '@nestjs/common'
import { map, Observable, Subject } from 'rxjs'
import { OrderStatus } from './order.entity'

export interface OrderStatusEvent {
	orderNumber: string
	status: OrderStatus
}

@Injectable()
export class OrderStatusStreamService {
	private readonly streams = new Map<string, Subject<OrderStatusEvent>>()

	register(orderNumbrer: string): void {
		this.streams.set(orderNumbrer, new Subject<OrderStatusEvent>())
	}

	watch(orderNumber: string): Observable<{ data: OrderStatusEvent }> {
		const stream = this.streams.get(orderNumber)

		if (!stream) {
			throw new NotFoundException(`Order ${orderNumber} not found`)
		}

		return stream.pipe(map((event) => ({ data: event })))
	}

	push(orderNumber: string, status: OrderStatus): void {
		const stream = this.streams.get(orderNumber)

		if (!stream) return

		stream.next({ orderNumber, status })

		if (status !== 'payment_pending') {
			stream.complete()
		}
	}
}
