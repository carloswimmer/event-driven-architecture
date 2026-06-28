import { z } from 'zod'

export const CustomerInfoSchema = z.object({
	email: z.string().email(),
})

export const PaymentSucceededSchema = z.object({
	reserveId: z.string().uuid(),
	amount: z.number().positive(),
	customerInfo: CustomerInfoSchema,
	orderNumber: z.string().uuid(),
})

export type PaymentSucceeded = z.infer<typeof PaymentSucceededSchema>
