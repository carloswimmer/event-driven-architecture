export const ROUTING_KEYS = {
	PAYMENT_REQUESTED: 'orders.payment.requested',
	EMAIL_DELIVERED: 'notifications.email.delivered',
	EMAIL_FAILED: 'notifications.email.not.delivered',
} as const

export const EXCHANGES = {
	COMMANDS: 'eda.commands',
} as const
