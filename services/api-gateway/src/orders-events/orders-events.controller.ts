import { Controller, Param, Sse } from '@nestjs/common'
import { OrderNumberParamSchema } from 'src/orders/create-order.schema'
import { OrderStatusStreamService } from 'src/orders/order-status-stream.service'
import { OrdersService } from 'src/orders/orders.service'

@Controller('orders-events')
export class OrdersEventsController {
	constructor(
		private readonly ordersService: OrdersService,
		private readonly statusStream: OrderStatusStreamService,
	) {}

	@Sse(':orderNumber/events')
	watchOrderStatus(@Param('orderNumber') orderNumber: string) {
		OrderNumberParamSchema.parse(orderNumber)
		this.ordersService.getOrder(orderNumber)

		return this.statusStream.watch(orderNumber)
	}
}
