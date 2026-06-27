import {
	PaymentFailed,
	PaymentFailedSchema,
	PaymentSucceeded,
	PaymentSucceededSchema,
	TOPICS,
} from '@eda/contracts'
import {
	Inject,
	Injectable,
	OnModuleDestroy,
	OnModuleInit,
} from '@nestjs/common'
import { ClientKafka } from '@nestjs/microservices'
import { firstValueFrom } from 'rxjs'
import { DomainEventPublisher } from './domain-event.publisher'

@Injectable()
export class KafkaDomainEventPublisher
	implements DomainEventPublisher, OnModuleInit, OnModuleDestroy
{
	constructor(@Inject('KAFKA_SERVICE') private readonly kafka: ClientKafka) {}

	async onModuleInit() {
		await this.kafka.connect()
	}

	async publishPaymentSucceeded(event: PaymentSucceeded): Promise<void> {
		const payload = PaymentSucceededSchema.parse(event)
		await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_SUCCEEDED, payload))
	}

	async publishPaymentFailed(event: PaymentFailed): Promise<void> {
		const payload = PaymentFailedSchema.parse(event)
		await firstValueFrom(this.kafka.emit(TOPICS.PAYMENT_FAILED, payload))
	}

	async onModuleDestroy() {
		await this.kafka.close()
	}
}
