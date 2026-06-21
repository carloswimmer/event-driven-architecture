import { Injectable } from '@nestjs/common'

@Injectable()
export class IdempotencyStore {
	private readonly processed = new Set<string>()

	isDuplicate(id: string): boolean {
		return this.processed.has(id)
	}

	markProcessed(id: string): void {
		this.processed.add(id)
	}
}
