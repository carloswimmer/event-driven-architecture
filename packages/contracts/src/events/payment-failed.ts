import { z } from 'zod'

export const PaymentFailedSchema = z.object({
	orderNumber: z.string().uuid(),
	reserveId: z.string().uuid(),
	reason: z.string().min(1),
})

export type PaymentFailed = z.infer<typeof PaymentFailedSchema>
