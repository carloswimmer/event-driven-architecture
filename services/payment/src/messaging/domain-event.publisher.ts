import { PaymentFailed, PaymentSucceeded } from '@eda/contracts'

export const DOMAIN_EVENT_PUBLISHER = Symbol('DOMAIN_EVENT_PUBLISHER')

export interface DomainEventPublisher {
	publishPaymentSucceeded(event: PaymentSucceeded): Promise<void>
	publishPaymentFailed(event: PaymentFailed): Promise<void>
}
