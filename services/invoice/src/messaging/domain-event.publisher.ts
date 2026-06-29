import { InvoiceCreated } from '@eda/contracts'

export const DOMAIN_EVENT_PUBLISHER = Symbol('DOMAIN_EVENT_PUBLISHER')

export interface DomainEventPublisher {
	publishInvoiceCreated(event: InvoiceCreated): Promise<void>
}
