import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AvailabilityModule } from './availability/availability.module';
import { InventoryService } from './inventory/inventory.service';
import { PaymentEventsController } from './payment-events/payment-events.controller';

@Module({
  imports: [AvailabilityModule],
  controllers: [AppController, PaymentEventsController],
  providers: [AppService, InventoryService],
})
export class AppModule {}
