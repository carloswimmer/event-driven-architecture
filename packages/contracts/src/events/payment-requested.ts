import { z } from 'zod'

export const PaymentRequestedSchema = z.object({
	reserveId: z.string().uuid(),
	orderNumber: z.string().uuid(),
	amount: z.number().positive(),
	customerEmail: z.string().email(),
})

export type PaymentRequested = z.infer<typeof PaymentRequestedSchema>
