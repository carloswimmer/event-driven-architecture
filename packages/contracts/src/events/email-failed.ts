import z from 'zod'

export const EmailFailedSchema = z.object({
	invoiceId: z.string().uuid(),
	orderNumber: z.string().uuid(),
	email: z.string().email(),
	reason: z.string(),
})

export type EmailFailed = z.infer<typeof EmailFailedSchema>
