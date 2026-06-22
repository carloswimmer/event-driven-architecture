import { EXCHANGES } from '@eda/contracts'
import { Module } from '@nestjs/common'
import { ClientsModule, Transport } from '@nestjs/microservices'
import { requireEnv } from 'src/common/env'
import { PAYMENT_COMMAND_PUBLISHER } from 'src/messaging/payment-command.publisher'
import { RabbitMqPaymentCommandPublisher } from 'src/messaging/rabbitmq-payment-command.publisher'
import { OrdersEventsController } from 'src/orders-events/orders-events.controller'
import { PaymentEventsHandler } from 'src/payment-events/payment-events.controller'
import { InMemoryOrdersRepository } from './in-memory-orders.repository'
import { OrderStatusStreamService } from './order-status-stream.service'
import { OrdersController } from './orders.controller'
import { ORDERS_REPOSITORY } from './orders.repository'
import { OrdersService } from './orders.service'

const clientsRegister = ClientsModule.register([
	{
		name: 'RABBITMQ_COMMANDS',
		transport: Transport.RMQ,
		options: {
			urls: [requireEnv('RABBITMQ_URL')],
			queue: 'orders.payment.requested',
			noAssert: true,
			queueOptions: { durable: true },
			wildcards: true,
			exchange: EXCHANGES.COMMANDS,
			exchangeTypes: 'topic',
		},
	},
])

@Module({
	imports: [clientsRegister],
	controllers: [OrdersController, OrdersEventsController, PaymentEventsHandler],
	providers: [
		OrdersService,
		OrderStatusStreamService,
		{ provide: ORDERS_REPOSITORY, useClass: InMemoryOrdersRepository },
		{
			provide: PAYMENT_COMMAND_PUBLISHER,
			useClass: RabbitMqPaymentCommandPublisher,
		},
	],
})
export class OrdersModule {}
