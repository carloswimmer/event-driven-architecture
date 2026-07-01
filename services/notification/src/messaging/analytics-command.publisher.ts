import { EmailDelivered, EmailFailed } from '@eda/contracts'

export const ANALYTICS_COMMAND_PUBLISHER = Symbol('ANALYTICS_COMMAND_PUBLISHER')

export interface AnalyticsCommandPublisher {
	publishEmailDelivered(payload: EmailDelivered): Promise<void>
	publishEmailFailed(payload: EmailFailed): Promise<void>
}
