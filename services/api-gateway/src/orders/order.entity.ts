export type OrderStatus =
	| 'payment_pending'
	| 'payment_succeeded'
	| 'payment_failed'

export interface Order {
	orderNumber: string
	reserveId: string
	productId: string
	amount: number
	customerEmail: string
	status: OrderStatus
}

export interface CreateOrderInput {
	productId: string
	customerEmail: string
	amount: number
}
