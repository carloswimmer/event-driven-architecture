export interface SendMailRequestDto {
	personalizations: Personalization[]
	subject: string
}

interface Personalization {
	to: EmailAddress[]
	custom_args?: Record<string, string>
}

interface EmailAddress {
	email: string
}

export interface SendMailResponseDto {
	recipient: string
	status: 'accepted' | 'bounce' | 'dropped'
}
