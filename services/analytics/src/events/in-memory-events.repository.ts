import { Injectable } from '@nestjs/common'
import { EventsRepository, StoredEvent } from './events.repository'

@Injectable()
export class InMemoryEventsRepository implements EventsRepository {
	private readonly events: StoredEvent[] = []

	append(type: string, payload: unknown): void {
		this.events.push({ type, payload, at: new Date().toISOString() })
	}

	list(): StoredEvent[] {
		return this.events
	}
}
