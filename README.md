# Broker Service - Core Resilience & Orchestration

## Description
The heart of the system's resilience and business orchestration. Originally designed for retries, it now also manages successful business workflows via an Event-Driven architecture.

## Responsibilities

### 1. Resilience (Retry Mechanism)
- **Listeners**: Consumes from `order_retry_jobs`, `payments_retry_jobs`, and `product_retry_jobs`.
- **Chain of Responsibility**:
   - **Step A (Creation)**: Retries the original operation via Feign clients.
   - **Step B (Email)**: Sends error/success notifications.
   - **Step C (Update)**: Updates SQL tracking record.
   - **Step D (Mongo)**: Persists final audit in MongoDB.

### 2. Business Orchestration (New Workflows)
- **PaymentReceivedListener**:
    - Listens to `payment_received_events`.
    - Calculates **Pending Balance** for partial payments.
    - Triggers shipping logic only when the order is 100% paid.
- **InventoryUpdateListener**:
    - Listens to `inventory_update_events`.
    - Automatically reduces stock in Product Service after order creation/modification.
- **OrderStatusChangedListener**:
    - Listens to `order_status_changed_events`.
    - Notifies customers about status updates and registers completed orders for shipping.
- **ShippingScheduler**:
    - Runs every 10 seconds to process the `envios` table and send final delivery confirmations.

## Persistence
- **PostgreSQL**: Stores `retry_jobs`, `envios` (shipping), and `notification_log` (idempotency).
- **MongoDB**: Stores final audit traces of processed jobs.

## Ports & Communication
- **Port**: `8084`
- **Tópicos Kafka**: `payment_received_events`, `inventory_update_events`, `order_status_changed_events`.
