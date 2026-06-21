import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { OrdersModule } from './orders/orders.module';
import { PaymentEventsController } from './payment-events/payment-events.controller';
import { OrdersEventsController } from './orders-events/orders-events.controller';
import { OrderStatusStreamService } from './order-status-stream/order-status-stream.service';

@Module({
  imports: [OrdersModule],
  controllers: [AppController, PaymentEventsController, OrdersEventsController],
  providers: [AppService, OrderStatusStreamService],
})
export class AppModule {}
