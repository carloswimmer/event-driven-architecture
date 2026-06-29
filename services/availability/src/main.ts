import 'dotenv/config'
import { NestFactory } from '@nestjs/core'
import { MicroserviceOptions, Transport } from '@nestjs/microservices'
import {
	FastifyAdapter,
	NestFastifyApplication,
} from '@nestjs/platform-fastify'
import { AppModule } from './app.module'
import { requireEnv } from './common/env'
import { ZodValidationExceptionFilter } from './common/zod-validation.filter'

async function bootstrap() {
	const app = await NestFactory.create<NestFastifyApplication>(
		AppModule,
		new FastifyAdapter(),
	)

	app.useGlobalFilters(new ZodValidationExceptionFilter())

	app.connectMicroservice<MicroserviceOptions>({
		transport: Transport.KAFKA,
		options: {
			client: {
				clientId: 'availability-service',
				brokers: requireEnv('KAFKA_BROKERS').split(','),
			},
			consumer: { groupId: 'availability-service' },
			subscribe: { fromBeginning: false },
		},
	})

	await app.startAllMicroservices()
	const port = Number(process.env.PORT ?? 3020)
	await app.listen(port, '0.0.0.0')
	console.log(`📝 availability-service listening on ${port}`)
}

bootstrap().catch((err) => {
	console.log(err)
	process.exit(1)
})
