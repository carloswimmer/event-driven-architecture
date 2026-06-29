import { InvoiceCreated, InvoiceCreatedSchema, TOPICS } from '@eda/contracts'
import { Inject, OnModuleDestroy, OnModuleInit } from '@nestjs/common'
import { ClientKafka } from '@nestjs/microservices'
import { firstValueFrom } from 'rxjs'
import { DomainEventPublisher } from './domain-event.publisher'

export class KafkaDomainEventPublisher
	implements DomainEventPublisher, OnModuleInit, OnModuleDestroy
{
	constructor(@Inject('KAFKA_SERVICE') private readonly kafka: ClientKafka) {}

	async onModuleInit() {
		await this.kafka.connect()
	}

	async publishInvoiceCreated(event: InvoiceCreated): Promise<void> {
		const payload = InvoiceCreatedSchema.parse(event)
		await firstValueFrom(this.kafka.emit(TOPICS.INVOICE_CREATED, payload))
	}

	async onModuleDestroy() {
		await this.kafka.close()
	}
}
