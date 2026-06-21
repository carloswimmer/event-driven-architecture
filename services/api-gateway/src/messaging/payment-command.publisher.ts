import { PaymentRequested } from '@eda/contracts'

export const PAYMENT_COMMAND_PUBLISHER = Symbol('PAYMENT_COMMAND_PUBLISHER')

export interface PaymentCommandPublisher {
	publishPaymentRequested(payload: PaymentRequested): Promise<void>
}
