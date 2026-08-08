# Architecture Guide

This document explains the platform from the outside in. Start with the one-sentence model, then follow the concrete order example.

## One-Sentence Model

The Order Service saves an order in PostgreSQL, Debezium detects that database change, Kafka distributes the change, and independent Go workers build inventory, notification, and analytics projections.

```text
REST request
    -> PostgreSQL transaction
    -> PostgreSQL WAL
    -> Debezium CDC
    -> Kafka topic
    -> independent Go consumers
    -> PostgreSQL projections and Redis state
```

This is primarily a **real-time CDC and event-processing platform**. It is not currently a data lake because it does not archive events to object storage such as S3 or MinIO.

## The Main Idea

There are two kinds of data in this project:

| Data | Meaning | System of record |
|---|---|---|
| Order data | What the customer purchased | PostgreSQL |
| CDC event | A record that the database changed | Kafka |
| Reservation lookup | Fast inventory status lookup | Redis cache, PostgreSQL remains authoritative |
| Analytics projection | A read model for reporting | `analytics_order_events` |
| Notification projection | Notification delivery status | `notifications` |

Kafka is not the database for orders. Redis is not the database for orders. PostgreSQL is the source of truth for the order transaction.

## Runtime Components

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Docker Compose                                                        │
│                                                                      │
│  ┌───────────┐       ┌───────────────┐       ┌────────────────────┐  │
│  │ Dashboard │──────▶│ Order Service │──────▶│ PostgreSQL          │  │
│  │ Nginx     │ REST  │ Go HTTP API   │ SQL   │ orders/order_items  │  │
│  │ :3000     │       │ :8080         │       │ WAL enabled         │  │
│  └───────────┘       └───────┬───────┘       └─────────┬──────────┘  │
│                              │                         │             │
│                              │ dashboard observer     │ logical WAL │
│                              ▼                         ▼             │
│                        ┌───────────┐           ┌───────────┐        │
│                        │ Kafka     │◀──────────│ Debezium  │        │
│                        │ :9092     │           │ :8083     │        │
│                        └─────┬─────┘           └───────────┘        │
│                              │                                      │
│             ┌────────────────┼────────────────┐                     │
│             ▼                ▼                ▼                     │
│       ┌────────────┐  ┌──────────────┐  ┌──────────────┐             │
│       │ Inventory  │  │ Notification │  │ Analytics    │             │
│       │ Go worker  │  │ Go worker    │  │ Go worker    │             │
│       └─────┬──────┘  └──────┬───────┘  └──────┬───────┘             │
│             │                │                │                     │
│             ▼                ▼                ▼                     │
│          Redis          PostgreSQL        PostgreSQL                 │
│       fast lookup      notifications    analytics projection         │
│                                                                      │
│  kafka-init creates the required topics before Debezium starts.      │
└──────────────────────────────────────────────────────────────────────┘
```

### Why the Order Service Does Not Publish to Kafka

The Order Service performs one database transaction. Debezium reads the committed PostgreSQL WAL change and publishes it to Kafka.

```text
Good:
  commit PostgreSQL
      -> Debezium publishes change

Risky dual write:
  write PostgreSQL
  publish Kafka manually
```

With the risky approach, the database write could succeed while the Kafka publish fails, or the Kafka publish could succeed while the database transaction rolls back. Debezium provides the database-to-event bridge without adding a second write to the request handler.

## One Order: Complete Example

Assume the client sends this request:

```http
POST /orders
Content-Type: application/json
```

```json
{
  "customer_id": "cust-101",
  "items": [
    {
      "product_id": "prod-1",
      "quantity": 2,
      "unit_price_cents": 1500
    }
  ]
}
```

### Step 1: Nginx forwards the request

The browser talks to the dashboard at port `3000`. Nginx forwards `/api/orders` to the Order Service:

```text
Browser:       POST http://localhost:3000/api/orders
Nginx proxy:   POST http://order-service:8080/orders
```

This keeps browser requests on one origin and avoids CORS configuration during local development.

### Step 2: The Order Service validates and calculates

The application service checks:

```text
customer_id is present
at least one item exists
product_id is present
quantity > 0
unit_price_cents >= 0
```

It calculates:

```text
2 × 1500 cents = 3000 cents
3000 cents     = $30.00
```

The service creates an order ID:

```text
7a7c19da-3178-4d65-813f-d24cbbcb404b
```

### Step 3: PostgreSQL commits one transaction

The repository writes both tables atomically:

```sql
INSERT INTO orders (id, customer_id, total_cents)
VALUES ('7a7c19da-3178-4d65-813f-d24cbbcb404b', 'cust-101', 3000);

INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
VALUES ('7a7c19da-3178-4d65-813f-d24cbbcb404b', 'prod-1', 2, 1500);

COMMIT;
```

If the item insert fails, the order insert is rolled back too.

The HTTP response is returned immediately:

```json
{
  "id": "7a7c19da-3178-4d65-813f-d24cbbcb404b",
  "customer_id": "cust-101",
  "status": "PENDING",
  "total_cents": 3000,
  "items": [
    {
      "product_id": "prod-1",
      "quantity": 2,
      "unit_price_cents": 1500
    }
  ]
}
```

### Step 4: PostgreSQL writes its WAL

PostgreSQL records the committed change in its **Write-Ahead Log**. The Compose configuration enables logical decoding:

```yaml
command: ["postgres", "-c", "wal_level=logical"]
```

The WAL is not a normal application table. It is PostgreSQL's durable change log used for recovery, replication, and CDC.

### Step 5: Debezium reads the WAL

The connector watches:

```json
"table.include.list": "public.orders,public.order_items"
```

For the `orders` insert, Debezium publishes a record to:

```text
platform.public.orders
```

For the item insert, it publishes a separate record to:

```text
platform.public.order_items
```

### Step 6: Kafka distributes the order event

The raw event is delivered independently to these consumer groups:

```text
platform.public.orders
    ├── inventory-service
    ├── notification-service
    ├── analytics-service
    └── dashboard-observer
```

Different groups receive their own copy. The services do not steal events from one another.

### Step 7: Each worker creates its projection

The same raw order event produces different effects:

```text
Inventory:
  inventory_reservations(order_id, status='RESERVED')
  Redis inventory:order:{order_id} = RESERVED

Notification:
  notifications(order_id, status='SENT')

Analytics:
  analytics_order_events(order_id, customer_id, total_cents)
```

These projections are intentionally different views of the same event.

## Kafka Topics and Consumer Groups

### Data Topics

| Topic | Created by | Current consumer |
|---|---|---|
| `platform.public.orders` | Debezium | All three workers and dashboard observer |
| `platform.public.order_items` | Debezium | Created, not currently consumed by workers |

### Kafka Connect Topics

| Topic | Purpose | Policy |
|---|---|---|
| `connect_configs` | Connector configuration | Compact |
| `connect_offsets` | Debezium source offsets | Compact |
| `connect_statuses` | Connector/task status | Compact |
| `schemahistory.platform` | Debezium schema history | Compact |

`kafka-init` creates these topics explicitly. It is a short-lived initialization container and should exit with code `0`.

### Consumer Group Example

Suppose Kafka receives event `E1`:

```text
E1 = order 7a7c19da-3178-4d65-813f-d24cbbcb404b was created
```

Kafka tracks progress separately:

```text
inventory-service      processed E1
notification-service   processed E1
analytics-service      processed E1
dashboard-observer     displayed E1
```

Each group has its own offset. Restarting Analytics does not reset Inventory's progress.

## Worker Architecture

`cmd/worker-service/main.go` is a composition root. The actual consumer is under `internal/worker`.

```text
cmd/worker-service/main.go
    │
    ├── load WORKER_ROLE
    ├── create PostgreSQL pool
    ├── optionally create Redis client
    ├── create worker.Handler
    └── create worker.Consumer

internal/worker/consumer.go
    │
    ├── FetchMessage from Kafka
    ├── put message on buffered channel
    ├── four goroutines process messages
    └── commit offset after success

internal/worker/handler.go
    │
    ├── decode Debezium JSON
    ├── calculate stable event ID from table + PostgreSQL LSN
    ├── claim event in processed_events
    ├── write role-specific projection
    └── update Redis for inventory
```

### Why Four Workers?

One worker processes one message at a time. Four workers can process multiple messages concurrently:

```text
Kafka reader
    │
    ▼
buffered channel
    ├── worker 1 -> order A
    ├── worker 2 -> order B
    ├── worker 3 -> order C
    └── worker 4 -> order D
```

The pool is bounded so a traffic spike does not create unlimited goroutines or overload PostgreSQL.

Kafka partitions provide distributed parallelism across service instances. Goroutines provide concurrent processing inside one instance.

## Idempotency and Failure Behavior

Kafka uses at-least-once delivery. A worker can receive the same event more than once, for example:

```text
1. Worker receives event
2. Worker writes projection
3. Process crashes before offset commit
4. Kafka delivers event again
```

The worker uses `processed_events`:

```sql
INSERT INTO processed_events (consumer_name, event_id)
VALUES ('analytics', 'orders:27019416')
ON CONFLICT DO NOTHING;
```

The event claim and SQL projection run in the same PostgreSQL transaction. If the projection fails, the claim rolls back and the event can be retried.

The stable event ID uses the PostgreSQL log sequence number (LSN), so two changes to the same order are not incorrectly treated as the same event.

## Repository Architecture

The `cmd` directories are intentionally small. They are **composition roots**, not places for business logic.

```text
cmd/                         Start processes and wire dependencies
  order-service/main.go
  worker-service/main.go

internal/config/             Read environment configuration
internal/platform/           PostgreSQL/Redis infrastructure adapters
internal/order/              Order domain and use cases
  domain.go                  Request, item, response, validation types
  service.go                 Validation, total calculation, ID generation
  ports.go                   Repository interface
  postgres_repository.go     PostgreSQL transaction implementation
  http_handler.go            REST adapter
internal/events/             Dashboard Kafka observer and memory store
internal/worker/             Kafka event parsing, projections, worker pool
```

### Request Direction

```text
HTTP handler
    -> order.Service
        -> order.Repository interface
            -> PostgreSQL repository
```

The service does not know about HTTP or SQL. This makes business rules testable with a fake repository.

### Event Direction

```text
Kafka consumer
    -> worker.Handler
        -> PostgreSQL transaction
        -> Redis cache for inventory
```

## Current Scope and Production Target

### Current Local Implementation

- One Kafka broker in KRaft mode
- One PostgreSQL instance
- One Redis instance
- Raw Debezium topics consumed directly by workers
- Shared database for the demonstration
- Simulated notification marked `SENT`
- Inventory reservation is order-level
- Dashboard stores only the latest event in process memory

### Production Target

```text
API Gateway / Ingress
    -> Order Service
    -> managed PostgreSQL primary + replicas
    -> Debezium Connect cluster
    -> Kafka cluster with TLS/SASL and replication factor 3
    -> versioned event relay / schema registry
    -> independently deployed consumers
    -> per-service databases and managed Redis
```

Production should add:

- A CDC-to-application-event relay
- A versioned `order.created` contract containing order items
- Explicit retry and dead-letter topics
- Schema Registry compatibility checks
- Authentication, authorization, TLS, and secret management
- Prometheus metrics and consumer-lag alerting
- Separate database credentials and service ownership

## Production Guardrails

The local Compose file intentionally exposes infrastructure ports so the project is easy to inspect. A production deployment must not copy that behavior directly.

| Area | Local demonstration | Production expectation |
|---|---|---|
| Secrets | `.env` values and demo credentials | Secret manager, rotation, no secrets in Git |
| PostgreSQL | One shared database | Managed/HA database and least-privilege service credentials |
| Kafka | One broker, plaintext, one replica | Multiple brokers, TLS/SASL, replication factor 3 |
| Redis | One container with a volume | Managed Redis/cluster, encrypted connection |
| Topics | `kafka-init` creates one-partition topics | Versioned topic provisioning and compatibility checks |
| API | Local HTTP | TLS termination, authentication, authorization, rate limits |
| Observability | Docker logs and dashboard | Metrics, traces, centralized logs, alerts, consumer lag |
| Deployment | Docker Compose | Kubernetes or managed container platform |

The production example at `deployments/compose/docker-compose.prod.example.yml` demonstrates safer container defaults, but it is a template rather than a complete production deployment.

## How to Read the Code

For an order request, read files in this order:

1. `cmd/order-service/main.go` — see what is wired together.
2. `internal/order/http_handler.go` — see how HTTP is decoded and responded to.
3. `internal/order/service.go` — see validation, total calculation, and ID creation.
4. `internal/order/postgres_repository.go` — see the transaction and SQL.
5. `deployments/debezium/connector.json` — see how PostgreSQL tables become Kafka topics.
6. `cmd/worker-service/main.go` — see worker composition.
7. `internal/worker/consumer.go` — see the worker pool and offset commit.
8. `internal/worker/handler.go` — see idempotency and projections.
