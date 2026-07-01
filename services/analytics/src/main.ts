import 'dotenv/config'
import { EXCHANGES } from '@eda/contracts'
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
				clientId: 'analytics-service',
				brokers: requireEnv('KAFKA_BROKERS').split(','),
			},
			consumer: { groupId: 'analytics-service' },
			subscribe: { fromBeginning: false },
		},
	})

	app.connectMicroservice<MicroserviceOptions>({
		transport: Transport.RMQ,
		options: {
			urls: [requireEnv('RABBITMQ_URL')],
			queue: 'notifications.email.delivered',
			noAssert: true,
			noAck: false,
			prefetchCount: 1,
			queueOptions: { durable: true },
			wildcards: true,
			exchange: EXCHANGES.COMMANDS,
			exchangeType: 'topic',
		},
	})

	app.connectMicroservice<MicroserviceOptions>({
		transport: Transport.RMQ,
		options: {
			urls: [requireEnv('RABBITMQ_URL')],
			queue: 'notifications.email.failed',
			noAssert: true,
			noAck: false,
			prefetchCount: 1,
			queueOptions: { durable: true },
			wildcards: true,
			exchange: EXCHANGES.COMMANDS,
			exchangeType: 'topic',
		},
	})

	app.startAllMicroservices()
	const port = Number(process.env.PORT ?? 3030)
	await app.listen(port, '0.0.0.0')
	console.log(`📈 analytics-service listening on ${port}`)
}

bootstrap().catch((err) => {
	console.error(err)
	process.exit(1)
})
