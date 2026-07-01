import z from 'zod'

export const EmailDeliveredSchema = z.object({
	invoiceId: z.string().uuid(),
	orderNumber: z.string().uuid(),
	email: z.string().email(),
	sgMessageId: z.string(),
})

export type EmailDelivered = z.infer<typeof EmailDeliveredSchema>
