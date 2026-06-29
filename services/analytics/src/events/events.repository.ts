export interface StoredEvent {
	type: string
	payload: unknown
	at: string
}

export const EVENTS_REPOSITORY = Symbol('EVENT_REPOSITORY')

export interface EventsRepository {
	append(type: string, payload: unknown): void
	list(): StoredEvent[]
}
