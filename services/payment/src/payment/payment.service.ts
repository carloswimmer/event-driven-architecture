import { Injectable } from '@nestjs/common'

@Injectable()
export class PaymentService {
	public async processPaymentRequested(data: unknown) {}

	public async publishPaymentFailed(orderNumber: string, type: string) {}
}
