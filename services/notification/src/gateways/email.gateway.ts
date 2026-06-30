export const EMAIL_GATEWAY = Symbol('EMAIL_GATEWAY')

export interface EmailGateway {
	sendInvoiceEmail(
		email: string,
		invoiceId: string,
		orderNumber: string,
	): Promise<void>
}
