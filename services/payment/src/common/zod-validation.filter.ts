import {
	ArgumentsHost,
	Catch,
	ExceptionFilter,
	HttpStatus,
} from '@nestjs/common'
import { FastifyReply } from 'fastify'
import { ZodError } from 'zod'

@Catch(ZodError)
export class ZodValidationExceptionFilter implements ExceptionFilter {
	catch(error: ZodError, host: ArgumentsHost) {
		const response = host.switchToHttp().getResponse<FastifyReply>()

		response.status(HttpStatus.BAD_REQUEST).send({
			statusCode: HttpStatus.BAD_REQUEST,
			error: 'Bad Request',
			message: 'Validation failed',
			issues: error.issues.map((issue) => ({
				path: issue.path.join('.'),
				message: issue.message,
			})),
		})
	}
}
