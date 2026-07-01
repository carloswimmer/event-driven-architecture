import { Module } from '@nestjs/common'
import { EventsController } from 'src/events/events.controller'
import { EVENTS_REPOSITORY } from 'src/events/events.repository'
import { EventsService } from 'src/events/events.service'
import { InMemoryEventsRepository } from 'src/events/in-memory-events.repository'
import { KafkaEventsHandler } from 'src/kafka-events/kafka-events.handler'
import { RabbitMqEventsHandler } from 'src/rabbitmq-events/rabbitmq-events.handler'

@Module({
	controllers: [EventsController, KafkaEventsHandler, RabbitMqEventsHandler],
	providers: [
		EventsService,
		{ provide: EVENTS_REPOSITORY, useClass: InMemoryEventsRepository },
	],
})
export class AnalyticsModule {}
