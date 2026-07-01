import 'dotenv/config'
import { NestFactory } from '@nestjs/core'
import {
	FastifyAdapter,
	NestFastifyApplication,
} from '@nestjs/platform-fastify'
import { AppModule } from './app.module'

async function bootstrap() {
	const app = await NestFactory.create<NestFastifyApplication>(
		AppModule,
		new FastifyAdapter(),
	)
	const port = Number(process.env.PORT ?? 3002)
	await app.listen(port, '0.0.0.0')
	console.log(`💌 sendgrid-mock listening on ${port}`)
}

bootstrap().catch((err) => {
	console.error(err)
	process.exit(1)
})
