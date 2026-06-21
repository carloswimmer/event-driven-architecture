import {
	PaymentRequested,
	PaymentRequestedSchema,
	ROUTING_KEYS,
} from '@eda/contracts'
import { Inject, Injectable } from '@nestjs/common'
import { ClientProxy } from '@nestjs/microservices'
import { firstValueFrom } from 'rxjs'
import { PaymentCommandPublisher } from './payment-command.publisher'

@Injectable()
export class RabbitMqPaymentCommandPublisher
	implements PaymentCommandPublisher
{
	constructor(
		@Inject('RABBITMQ_COMMANDS') private readonly client: ClientProxy,
	) {}

	async publishPaymentRequested(payload: PaymentRequested): Promise<void> {
		const command = PaymentRequestedSchema.parse(payload)

		return await firstValueFrom(
			this.client.emit(ROUTING_KEYS.PAYMENT_REQUESTED, command),
		)
	}
}
