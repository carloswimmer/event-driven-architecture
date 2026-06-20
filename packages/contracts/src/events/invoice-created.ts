import { z } from "zod";
import { CustomerInfoSchema } from "./payment-succeeded";

export const InvoiceCreatedSchema = z.object({
  value: z.number().positive(),
  customerInfo: CustomerInfoSchema,
  orderNumber: z.string().uuid(),
  invoiceId: z.string().uuid()
})

export type InvoiceCreated = z.infer<typeof InvoiceCreatedSchema>