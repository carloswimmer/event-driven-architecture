import z from 'zod'

export const SendGridWebhookEventSchema = z.object({
	event: z.enum([
		'processed',
		'deliverd',
		'bounce',
		'dropped',
		'deferred',
		'open',
		'click',
	]),
	email: z.string().email(),
	timestamp: z.number(),
	sg_message_id: z.string(),
	invoiceId: z.string().uuid().optional(),
	orderNumber: z.string().uuid().optional(),
	reason: z.string().optional(),
	response: z.string().optional(),
	type: z.string().optional(),
})

export const SendGridWebhookPayloadSchema = z.array(SendGridWebhookEventSchema)

export type SendGridWebhookEvent = z.infer<typeof SendGridWebhookEventSchema>
