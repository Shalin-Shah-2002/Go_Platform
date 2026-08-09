# Local Development and Operations Guide

This guide is a copy-paste path for running, testing, inspecting, and resetting the platform.

## Prerequisites

- Docker Desktop running
- Docker Compose v2
- `curl` for API tests
- A terminal

Go is only required for running tests directly on the host. Docker builds the Go services inside the image.

## Start the Platform

From the repository root:

```bash
cp .env.example .env
docker compose up --build -d
```

The stack starts:

| Service | Role | Port |
|---|---|---:|
| `dashboard` | Operator UI and reverse proxy | `3000` |
| `order-service` | REST API | `8080` |
| `postgres` | Order database and WAL | `5432` |
| `kafka` | Event broker | `9094` from host, `9092` inside Docker |
| `kafka-init` | Creates topics once | exits `0` |
| `debezium` | PostgreSQL CDC connector | `8083` |
| `debezium-init` | Registers the PostgreSQL source connector | exits `0` |
| `redis` | Inventory cache | `6379` |
| `inventory-service` | Inventory consumer | internal |
| `notification-service` | Notification consumer | internal |
| `analytics-service` | Analytics consumer | internal |

Check the containers:

```bash
docker compose ps
```

`kafka-init` and `debezium-init` should show `Exited (0)`. Long-running services should show `Up` or `Up (healthy)`.

## Register Debezium

The `debezium-init` container registers the source connector automatically. Inspect its output:

```bash
docker compose logs debezium-init
```

To register or update it manually:

```bash
curl -fsS -X PUT http://localhost:8083/connectors/platform-postgres/config \
  -H 'Content-Type: application/json' \
  --data @deployments/debezium/connector.json
```

Verify it:

```bash
curl -fsS http://localhost:8083/connectors/platform-postgres/status
```

Expected important values:

```json
{
  "connector": {"state": "RUNNING"},
  "tasks": [{"state": "RUNNING"}]
}
```

If the connector is missing after recreating Kafka, restart `debezium-init` or register it manually. Its configuration is stored in Kafka Connect's internal topic.

## Use the Dashboard

Open:

```text
http://localhost:3000
```

The dashboard can:

1. Check Order Service health.
2. Check Debezium status.
3. Create an order with one or more items.
4. Display the actual raw CDC event received from Kafka.

The dashboard is for local testing. Its latest-event view is an in-memory copy and is not a durable event archive.

## Test the REST API Directly

Create an order:

```bash
curl -fsS -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customer_id": "cust-101",
    "items": [
      {
        "product_id": "prod-1",
        "quantity": 2,
        "unit_price_cents": 1500
      }
    ]
  }'
```

Expected result:

```json
{
  "id": "uuid",
  "customer_id": "cust-101",
  "status": "PENDING",
  "total_cents": 3000,
  "items": [
    {
      "product_id": "prod-1",
      "quantity": 2,
      "unit_price_cents": 1500
    }
  ],
  "created_at": "timestamp"
}
```

Check service health:

```bash
curl -fsS http://localhost:8080/health
```

Read the latest observed CDC event:

```bash
curl -fsS http://localhost:8080/events/latest
```

## Inspect Kafka

List topics:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

Expected application topics:

```text
platform.public.orders
platform.public.order_items
```

Read order CDC events:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic platform.public.orders \
  --from-beginning \
  --max-messages 1
```

Read item CDC events:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic platform.public.order_items \
  --from-beginning \
  --max-messages 1
```

## Inspect PostgreSQL

Open `psql`:

```bash
```

Useful queries:

```sql
\dt

SELECT id, customer_id, status, total_cents, created_at
FROM orders
ORDER BY created_at DESC
LIMIT 10;

SELECT order_id, product_id, quantity, unit_price_cents
FROM order_items
ORDER BY id DESC
LIMIT 10;

SELECT consumer_name, count(*)
FROM processed_events
GROUP BY consumer_name;

SELECT * FROM notifications ORDER BY created_at DESC LIMIT 10;
SELECT * FROM inventory_reservations ORDER BY created_at DESC LIMIT 10;
SELECT * FROM analytics_order_events ORDER BY created_at DESC LIMIT 10;
```

Verify one order across projections:

```sql
SELECT 'notification' AS projection, count(*)
FROM notifications
WHERE order_id = 'PUT-ORDER-UUID-HERE'
UNION ALL
SELECT 'inventory', count(*)
FROM inventory_reservations
WHERE order_id = 'PUT-ORDER-UUID-HERE'
UNION ALL
SELECT 'analytics', count(*)
FROM analytics_order_events
WHERE order_id = 'PUT-ORDER-UUID-HERE';
```

## Inspect Redis

List inventory keys:

```bash
```

Read one reservation:

```bash
```

Check its TTL:

```bash
```

The inventory worker stores `RESERVED` with a 24-hour expiration. PostgreSQL remains the durable reservation record.

## View Logs

All services:

```bash
```

Order API:

```bash
```

Workers:

```bash
```

CDC and Kafka:

```bash
```

## Common Problems

### Debezium status is `404`

Kafka Connect is running but the source connector has not been registered:

```bash
docker compose logs debezium-init
curl -X PUT http://localhost:8083/connectors/platform-postgres/config \
  -H 'Content-Type: application/json' \
  --data @deployments/debezium/connector.json
```

### Debezium is `UNASSIGNED` or its task is not running

Check the logs:

```bash
docker compose logs --tail=100 debezium
```

Check that Kafka and Debezium initialization completed:

```bash
docker compose ps kafka kafka-init debezium debezium-init
docker compose logs kafka-init
docker compose logs debezium-init
```

The internal Connect topics must exist and use `cleanup.policy=compact`. The current `kafka-init` service creates and configures them.

### No event appears

Check in this order:

```bash
curl http://localhost:8080/health
curl http://localhost:8083/connectors/platform-postgres/status
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose logs -f debezium
```

Then create a new order. The dashboard observer starts at the latest offset, so it displays new events observed after the Order Service starts.

### Tables do not exist

The init SQL runs automatically only for a new PostgreSQL volume. Apply it to an existing development volume:

```bash
docker compose exec -T postgres psql -U platform -d platform < migrations/001_init.sql
```

### Port already in use

Find the process using a port or change the host-side port in `docker-compose.yml`. The container ports remain the same.

### Worker appears to do nothing

Check its role and logs:

```bash
docker compose logs --tail=100 notification-service
docker compose logs --tail=100 analytics-service
```

Each worker reads `platform.public.orders`, uses a separate consumer group, and starts four workers.

## Reset Local Data

Stop containers but keep data:

```bash
docker compose down
```

Delete local PostgreSQL, Kafka, and Redis volumes:

```bash
docker compose down -v
docker compose up --build -d
```

This is destructive. Never use `down -v` against a production environment.

## Code Quality Checks

```bash
make check
```

This runs:

```text
gofmt
go vet ./...
go test ./...
docker compose config --quiet
```
