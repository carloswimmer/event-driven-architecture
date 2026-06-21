import { Body, Controller, Get, Param, Post } from '@nestjs/common'
import {
	CreateOrderSchema,
	OrderNumberParamSchema,
} from './create-order.schema'
import { OrdersService } from './orders.service'

@Controller('orders')
export class OrdersController {
	constructor(private readonly ordersService: OrdersService) {}

	@Post()
	createOrder(@Body() body: unknown) {
		const input = CreateOrderSchema.parse(body)
		return this.ordersService.createOrder(input)
	}

	@Get(':orderNumber')
	getOrder(@Param('orderNumber') orderNumber: string) {
		OrderNumberParamSchema.parse(orderNumber)
		return this.ordersService.getOrder(orderNumber)
	}
}
