import { resolve } from 'node:path'
import { config } from 'dotenv'

config({ path: resolve(__dirname, '../.env') })

import { NestFactory } from '@nestjs/core'
import {
	FastifyAdapter,
	type NestFastifyApplication,
} from '@nestjs/platform-fastify'
import { AppModule } from './app.module'

async function bootstrap() {
	const app = await NestFactory.create<NestFastifyApplication>(
		AppModule,
		new FastifyAdapter(),
	)
	const port = Number(process.env.PORT ?? 3001)
	await app.listen(port, '0.0.0.0')
	console.log(`stripe-mock listening on ${port}`)
}

bootstrap().catch((err) => {
	console.error(err)
	process.exit(1)
})
