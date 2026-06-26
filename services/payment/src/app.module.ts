import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PaymentModule } from './payment/payment.module';
import { PaymentConsumerController } from './payment-consumer/payment-consumer.controller';
import { WebhooksController } from './webhooks/webhooks.controller';

@Module({
  imports: [PaymentModule],
  controllers: [AppController, PaymentConsumerController, WebhooksController],
  providers: [AppService],
})
export class AppModule {}
