import { z } from 'zod'

export const StripeWebhookDataSchema = z.object({
	orderNumber: z.string().uuid(),
	amount: z.number().positive(),
	reserveId: z.string().uuid(),
	customerEmail: z.string().email(),
})

export const StripeWebhookSchema = z.discriminatedUnion('type', [
	z.object({
		type: z.literal('payment_intent.succeeded'),
		data: StripeWebhookDataSchema,
	}),
	z.object({
		type: z.literal('payment_intent.payment_failed'),
		data: StripeWebhookDataSchema,
	}),
])

export type StripeWebhook = z.infer<typeof StripeWebhookSchema>
