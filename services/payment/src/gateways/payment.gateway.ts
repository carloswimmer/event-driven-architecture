import { PaymentRequested } from '@eda/contracts'

export const PAYMENT_GATEWAY = Symbol('PAYMENT_GATEWAY')

export interface PaymentGateway {
	createPaymentIntent(input: PaymentRequested): Promise<void>
}
