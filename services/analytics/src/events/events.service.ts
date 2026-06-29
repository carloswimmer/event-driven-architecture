import { Inject, Injectable } from '@nestjs/common'
import { EVENTS_REPOSITORY, EventsRepository } from './events.repository'

@Injectable()
export class EventsService {
	constructor(
		@Inject(EVENTS_REPOSITORY)
		private readonly eventRepository: EventsRepository,
	) {}

	record(type: string, payload: unknown) {
		this.eventRepository.append(type, payload)
	}

	list() {
		return this.eventRepository.list()
	}
}
