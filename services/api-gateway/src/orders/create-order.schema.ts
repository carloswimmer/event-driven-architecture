import { z } from 'zod'

export const CreateOrderSchema = z.object({
	productId: z.string().min(1),
	customerEmail: z.string().email(),
	amount: z.number().positive(),
})

export type CreateOrderInput = z.infer<typeof CreateOrderSchema>

export const OrderNumberParamSchema = z.string().uuid()
