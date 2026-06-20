export interface CreatePaymentIntentRequestDto {
	orderNumber: string
	amount: number
	reserveId: string
	customerEmail: string
}

export interface CreatePaymentIntentResponseDto {
	id: string
	status: 'processing' | 'succeeded' | 'failed'
}
