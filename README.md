# Real-Time Data Integration Platform

A production-oriented event-processing platform that captures database changes in real time, streams them through Apache Kafka, and processes those events through independent Go microservices with Redis caching and a live operator dashboard.

> **Quick summary** — full walkthrough in [docs/request-flow-explained.md](docs/request-flow-explained.md).

- **One request's journey:** Browser → Nginx → Order Service → PostgreSQL → Debezium (CDC watches the WAL) → Kafka → 3 worker services (Inventory/Notification/Analytics) → PostgreSQL + Redis → dashboard.
- **Three separate things:** the **code** (`cmd/`, `internal/`), **your servers** (order-service + 3 workers + Nginx), and the **CDC service** (Debezium — a separate Java server, not your code, configured by `connector.json`).
- **What's in the repo:** `cmd/` (entry points), `internal/` (config, events, order domain, platform adapters, worker logic), `migrations/` (schema), `frontend/` (dashboard), `deployments/` (Debezium + production Compose), `docs/` (guides).
- **Key mechanics:** one DB transaction per order (`orders` + `order_items`), idempotency via `processed_events` (`ON CONFLICT DO NOTHING`), offsets committed only after success, Redis TTL cache for inventory, Kafka topics created by the `kafka-init` container (not in code), DB pool pinged at startup (`WaitForPostgres`) and on `/health`.

---

## Table of Contents

- [What We Built](#what-we-built)
- [Architecture](#architecture)
- [Detailed Documentation](#detailed-documentation)
- [Technology Stack & Rationale](#technology-stack--rationale)
- [How It Works](#how-it-works)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)
- [Kafka Topics & Events](#kafka-topics--events)
- [Consumer Design](#consumer-design)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Operations](#operations)
- [Production Deployment](#production-deployment)

---

## What We Built

A **complete event pipeline** that takes an order from a REST API, persists it in PostgreSQL, captures the insert using Change Data Capture (CDC), streams the change to Kafka, and processes that change through three independent Go consumer services — Inventory, Notification, and Analytics — each with its own Kafka consumer group, worker pool, Redis state, and idempotency protection.

## Detailed Documentation

Use these guides when learning or operating the project:

| Guide | What it explains |
|---|---|
| [Architecture Guide](docs/architecture.md) | Full architecture, one order from REST to Kafka, layers, topics, consumer groups, and production target |
| [Event Reference](docs/event-reference.md) | Debezium `schema`, `payload`, operation codes, topic mapping, LSN IDs, and application-event design |
| [Local Development and Operations](docs/operations.md) | Start, register Debezium, test, inspect PostgreSQL/Kafka/Redis, troubleshoot, and reset |
| [Production Architecture Notes](docs/architecture.md#current-scope-and-production-target) | What is demo-ready and what must change for production |

### Features

| Feature | Implementation |
|---|---|
| REST API | `POST /orders` with transactional persistence |
| Change Data Capture | Debezium reading PostgreSQL WAL |
| Event Streaming | Apache Kafka (KRaft mode, no ZooKeeper) |
| Consumer Groups | Independent groups per service |
| Worker Pools | 4 goroutines per consumer, buffered channels |
| Idempotent Processing | Dedicated `processed_events` table with ON CONFLICT |
| Offset Management | Commit only after successful processing |
| Redis Caching | Inventory reservation state with TTL |
| Operator Dashboard | Live UI with CDC event inspection |
| Health Checks | Every container and connector |
| Containerized | Full Docker Compose stack |
| Production Config | Separate Compose file, read-only containers, security options |

### Verified End-to-End Flow

```text
POST /orders (REST)
    ↓
PostgreSQL INSERT (transaction)
    ↓
Debezium reads WAL
    ↓
Kafka topic: platform.public.orders
    ↓
┌───────────────┬───────────────────┬──────────────────┐
│  Inventory    │  Notification     │  Analytics        │
│  Consumer     │  Consumer         │  Consumer         │
│               │                   │                   │
│  → PostgreSQL │  → PostgreSQL     │  → PostgreSQL     │
│  → Redis      │                   │                   │
└───────────────┴───────────────────┴──────────────────┘
```

---

## Architecture

```
                         Browser (localhost:3000)
                              │
                              ▼
                      ┌──────────────┐
                      │   Dashboard  │  (Nginx + static)
                      │   Nginx      │
                      └──────┬───────┘
                             │ proxy_pass
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      ┌───────────┐  ┌──────────┐  ┌──────────────┐
      │  Order    │  │ Debezium │  │  Kafka        │
      │  Service  │  │ Connect  │  │  Console      │
      │  :8080    │  │  :8083   │  │  Consumer     │
      └─────┬─────┘  └────┬─────┘  └──────────────┘
            │             │
            │             │ CDC
            ▼             │
    ┌───────────┐         │
    │ PostgreSQL│◄────────┘
    │  :5432    │  WAL
    └───────────┘
            │
            │ logical replication
            ▼
    ┌───────────┐
    │  Debezium │
    └─────┬─────┘
          │
          ▼
    ┌───────────┐
    │   Kafka   │  platform.public.orders
    │   :9094   │
    └─────┬─────┘
          │
    ┌─────┼─────────────┬─────────────┐
    ▼     ▼             ▼             ▼
 Inventory Notification Analytics Dashboard
 Service   Service      Service     Observer
    │
    ▼
  ┌───────┐
  │ Redis │
  │ :6379 │
  └───────┘
```

### Communication Patterns

| Pattern | Technology | Used For |
|---|---|---|
| Synchronous Request/Response | REST over HTTP | Client order creation |
| Asynchronous Event Streaming | Kafka consumer groups | Service decoupling |
| Change Data Capture | Debezium + pgoutput | Database-to-event bridge |
| Fast Key/Value State | Redis | Inventory reservation cache |
| Reverse Proxy | Nginx | Same-origin dashboard access |

The Order Service writes to PostgreSQL and **does not manually publish to Kafka**. Debezium detects the PostgreSQL insert automatically through the write-ahead log. This keeps the Order Service simple and avoids dual-write consistency problems.

Each consumer service uses a **separate Kafka consumer group** so every service receives its own copy of every event independently.

---

## Technology Stack & Rationale

### Go (Golang)

**Why:** High-performance compiled language with built-in concurrency via goroutines and channels. Ideal for Kafka consumers that need to process events concurrently. Single-binary deployment with no runtime dependency.

**How used:**
- `cmd/order-service` — REST API server with `net/http`
- `cmd/worker-service` — Kafka consumer with 4-worker goroutine pool

### PostgreSQL

**Why:** The durable source of truth. ACID transactions, UUID primary keys, foreign keys for data integrity, and built-in support for logical replication through `pgoutput`.

**How used:**
- `wal_level=logical` enables CDC
- `orders` and `order_items` tables use a single transaction per order
- Consumer projections in `processed_events`, `notifications`, `analytics_order_events`, `inventory_reservations`

### Debezium

**Why:** Industry-standard open-source CDC tool. Reads PostgreSQL's write-ahead log via logical decoding and publishes structured JSON events to Kafka. No application code needed to publish database changes.

**How used:**
- Debezium Connect 2.7.3 with PostgreSQL connector
- Watches `public.orders` and `public.order_items`
- Publishes to `platform.public.orders` and `platform.public.order_items`
- Replication slot: `platform_slot`

### Apache Kafka

**Why:** Durable, high-throughput event streaming platform. Decouples producers from consumers. Allows multiple independent consumer groups to process the same event stream. Survives consumer restarts without data loss.

**How used:**
- KRaft mode (no ZooKeeper dependency)
- Single-broker setup for development
- Topic: `platform.public.orders`
- Consumer groups: `inventory-service`, `notification-service`, `analytics-service`, `dashboard-observer`

### Redis

**Why:** In-memory key/value store for sub-millisecond reads. Suitable for frequently accessed state such as inventory reservations. Complements PostgreSQL by providing fast access without querying the primary database.

**How used:**
- Key pattern: `inventory:order:{order_id}`
- Value: `RESERVED`
- TTL: 24 hours

### Docker & Docker Compose

**Why:** Reproducible development environment. All infrastructure (PostgreSQL, Kafka, Debezium, Redis) and application services run in containers with private networking. No local installation needed.

**How used:**
- 11 containers/services: PostgreSQL, Kafka, Kafka topic initializer, Debezium, Debezium connector initializer, Redis, Order Service, Inventory, Notification, Analytics, Dashboard
- Dockerfile with multi-stage build
- Production Compose example with `read_only`, `no-new-privileges`, and image-based deployment

### Nginx

**Why:** Lightweight reverse proxy that serves the static dashboard and proxies API calls to the Order Service and Debezium Connect REST API. Avoids CORS complexity by keeping all browser requests same-origin.

**How used:**
- Serves static HTML/CSS/JS from `/frontend`
- Proxies `/api/*` to `order-service:8080`
- Proxies `/connectors/*` to `debezium:8083`

### Go Libraries

| Library | Purpose |
|---|---|
| `jackc/pgx/v5` | PostgreSQL driver with connection pooling |
| `segmentio/kafka-go` | Pure Go Kafka client (no librdkafka dependency) |
| `redis/go-redis/v9` | Redis client with context support |
| `google/uuid` | UUID generation for order and event IDs |

---

## How It Works

### Step 1: Order Creation

A client sends:

```json
POST /orders
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

The Order Service:

1. Validates the request (customer ID, items, quantities, prices)
2. Calculates `total_cents = 2 × 1500 = 3000`
3. Starts a PostgreSQL transaction
4. Inserts into `orders` table
5. Inserts each item into `order_items` table
6. Commits the transaction (both inserts succeed or both fail)
7. Returns `201 Created` with the order ID

### Step 2: Change Data Capture

PostgreSQL writes the inserts to its **Write-Ahead Log (WAL)**. Because `wal_level=logical` is configured, the WAL contains enough detail for Debezium to reconstruct row-level changes.

Debezium reads the logical replication stream through a **replication slot** named `platform_slot` and a **publication** named `platform_publication`. It converts the database change into a structured JSON event:

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": "be542f3a-46de-438a-a287-2560c20356d9",
      "customer_id": "cust-101",
      "status": "PENDING",
      "total_cents": 3000,
      "created_at": "2026-08-08T13:38:55.415673Z"
    },
    "source": {
      "db": "platform",
      "schema": "public",
      "table": "orders",
      "connector": "postgresql",
      "version": "2.7.3.Final"
    },
    "op": "c"
  }
}
```

The `op` field indicates the operation:
- `c` = create (INSERT)
- `u` = update (UPDATE)
- `d` = delete (DELETE)
- `r` = read (snapshot)

Debezium publishes this event to the Kafka topic `platform.public.orders`.

### Step 3: Kafka Event Distribution

Kafka stores the event durably in the topic. Because auto-topic-creation is enabled locally, the topic is created on first use. In production, topics should be created explicitly with appropriate partition counts and replication factors.

Each consumer service belongs to a **different consumer group**:
- `inventory-service`
- `notification-service`
- `analytics-service`
- `dashboard-observer`

Kafka delivers the same event to each group independently. Processing by one service does not prevent other services from receiving the event.

### Step 4: Consumer Processing

Each consumer starts **4 goroutine workers** connected by a **buffered channel**:

```go
messages := make(chan kafka.Message, 32)

for i := 0; i < 4; i++ {
    go worker(messages)
}

for {
    msg := reader.FetchMessage(ctx)
    messages <- msg
}
```

Each worker:

1. Receives a Kafka message from the channel
2. Parses the Debezium JSON envelope
3. Checks `processed_events` table for idempotency
4. Executes business logic
5. Commits the Kafka offset

### Step 5: Business Logic Per Service

#### Inventory Service

- Inserts into `inventory_reservations` with status `RESERVED`
- Writes Redis key: `inventory:order:{id} = RESERVED` (TTL: 24h)
- Uses `ON CONFLICT DO NOTHING` for duplicate protection

#### Notification Service

- Inserts into `notifications` with status `SENT`
- In production, this would call an email/SMS provider

#### Analytics Service

- Inserts into `analytics_order_events` with customer ID and order total
- In production, this would update dashboards and aggregate metrics

### Step 6: Idempotency Guarantee

Kafka guarantees **at-least-once** delivery. A message can be delivered more than once if a consumer crashes before committing its offset.

Each consumer writes to `processed_events`:

```sql
INSERT INTO processed_events (consumer_name, event_id)
VALUES ('analytics', 'be542f3a-46de-438a-a287-2560c20356d9')
ON CONFLICT DO NOTHING;
```

The composite primary key `(consumer_name, event_id)` ensures each event is processed exactly once per consumer, even if Kafka delivers it multiple times.

### Step 7: Offset Management

Offsets are committed **only after successful processing**:

```go
if err := processEvent(ctx, db, role, message.Value); err != nil {
    log.Error(...)
    continue  // Do NOT commit offset
}
reader.CommitMessages(ctx, message)  // Safe to commit
```

This prevents data loss: if processing fails, Kafka will redeliver the message.

---

## Project Structure

```
.
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile                          # Multi-stage Go build
├── Makefile                            # up, down, logs, test, fmt, check
├── README.md
├── docker-compose.yml                  # Local development stack (10 services)
├── go.mod                              # Go module and dependencies
├── go.sum
│
├── cmd/
│   ├── order-service/main.go           # Order-service composition root
│   └── worker-service/main.go          # Worker-service composition root
│
├── internal/
│   ├── config/                         # Environment-backed configuration
│   ├── events/                         # Kafka observer and event store
│   ├── order/                          # Domain, service, repository, HTTP adapter
│   ├── platform/                       # PostgreSQL and Redis adapters
│   └── worker/                         # Event model, projections, consumer pool
│
├── migrations/
│   └── 001_init.sql                    # Tables: orders, order_items,
│                                       #   processed_events, notifications,
│                                       #   analytics_order_events,
│                                       #   inventory_reservations
├── frontend/
│   ├── index.html                      # Operator dashboard
│   ├── styles.css                      # Responsive design (mobile + desktop)
│   ├── app.js                          # Order creation + live CDC viewer
│   └── nginx.conf                      # Reverse proxy configuration
│
├── deployments/
│   ├── debezium/
│   │   ├── connector.json              # PostgreSQL CDC connector config
│   │   └── register-connector.sh       # Connector registration helper
│   └── compose/
│       └── docker-compose.prod.example.yml  # Production deployment template
│
└── docs/
    ├── architecture.md                 # Production boundaries and rules
    └── operations.md                   # Runbook: health, logs, recovery
```

---

## Database Schema

### Core Tables

```sql
orders
├── id              UUID PRIMARY KEY
├── customer_id     TEXT NOT NULL
├── status          TEXT DEFAULT 'PENDING'
├── total_cents     BIGINT CHECK (>= 0)
└── created_at      TIMESTAMPTZ DEFAULT now()

order_items
├── id              BIGSERIAL PRIMARY KEY
├── order_id        UUID FK → orders(id) ON DELETE CASCADE
├── product_id      TEXT NOT NULL
├── quantity        INT CHECK (> 0)
└── unit_price_cents BIGINT CHECK (>= 0)
```

### Service Projection Tables

```sql
processed_events           -- Idempotency guard
├── consumer_name  TEXT    -- 'analytics' | 'notification' | 'inventory'
├── event_id       TEXT    -- order UUID
└── PRIMARY KEY (consumer_name, event_id)

notifications              -- Notification delivery records
├── id             BIGSERIAL PRIMARY KEY
├── order_id       UUID NOT NULL
├── status         TEXT NOT NULL
└── created_at     TIMESTAMPTZ DEFAULT now()

analytics_order_events     -- Analytics projection
├── order_id       UUID PRIMARY KEY
├── customer_id    TEXT NOT NULL
├── total_cents    BIGINT NOT NULL
└── created_at     TIMESTAMPTZ DEFAULT now()

inventory_reservations     -- Inventory reservation records
├── order_id       UUID PRIMARY KEY
├── status         TEXT NOT NULL
└── created_at     TIMESTAMPTZ DEFAULT now()
```

### Indexes

```sql
orders_created_at_idx ON orders (created_at DESC)
```

---

## Kafka Topics & Events

### Topics

| Topic | Source | Description |
|---|---|---|
| `platform.public.orders` | Debezium CDC | Order inserts, updates, deletes |
| `platform.public.order_items` | Debezium CDC | Order item inserts, updates, deletes |
| `connect_configs` | Kafka Connect | Connector configuration storage |
| `connect_offsets` | Kafka Connect | Connector offset storage |
| `connect_statuses` | Kafka Connect | Connector status storage |
| `schemahistory.platform` | Debezium | Schema evolution history |

### Consumer Groups

| Consumer Group | Service | Start Offset |
|---|---|---|
| `inventory-service` | Inventory | First offset (replay all) |
| `notification-service` | Notification | First offset (replay all) |
| `analytics-service` | Analytics | First offset (replay all) |
| `dashboard-observer` | Dashboard | Last offset (new only) |

### Event Envelope

Every Debezium event follows this structure:

```json
{
  "schema": { ... },
  "payload": {
    "before": null,
    "after": {
      "id": "uuid",
      "customer_id": "...",
      "status": "PENDING",
      "total_cents": 3000,
      "created_at": "..."
    },
    "source": {
      "version": "2.7.3.Final",
      "connector": "postgresql",
      "name": "platform",
      "db": "platform",
      "schema": "public",
      "table": "orders",
      "txId": 748,
      "lsn": 26728440
    },
    "op": "c",
    "ts_ms": 1786196335695
  }
}
```

---

## Consumer Design

### Worker Pool Architecture

```
Kafka Reader (main goroutine)
    │
    │ FetchMessage()
    ▼
┌──────────────────────┐
│  Buffered Channel    │  capacity: 32
│  chan kafka.Message  │
└──────┬───────┬───────┘
       │       │
   ┌───▼──┐ ┌──▼────┐ ┌──▼────┐ ┌──▼────┐
   │Worker│ │Worker │ │Worker │ │Worker │
   │  0   │ │  1    │ │  2    │ │  3    │
   └──┬───┘ └──┬────┘ └──┬────┘ └──┬────┘
      │        │         │         │
      ▼        ▼         ▼         ▼
  ┌─────────────────────────────────────────┐
  │ 1. Parse Debezium JSON                  │
  │ 2. Check idempotency                    │
  │ 3. Execute business logic               │
  │ 4. Commit offset                        │
  └─────────────────────────────────────────┘
```

### Idempotency Flow

```
Receive Kafka message
    │
    ▼
Unmarshal Debezium JSON
    │
    ▼
Is op == "d" (delete) or after == null?
    ├── Yes → Skip (no action needed)
    │
    ▼ No
Try INSERT INTO processed_events (consumer_name, event_id)
    │
    ├── Row inserted? (first time seeing this event)
    │       │
    │       ▼
    │   Execute business logic
    │       │
    │       ▼
    │   Commit Kafka offset
    │
    └── Conflict? (already processed)
            │
            ▼
        Skip (idempotent, no duplicate work)
```

### Offset Commit Strategy

```
Fetch message from Kafka
    │
    ▼
Process message
    │
    ├── Success → CommitMessages()  ✓
    │
    └── Error → Log error, do NOT commit
                 (Kafka will redeliver on restart)
```

---

## Getting Started

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- curl (optional, for CLI testing)

### 1. Clone and start

```bash
cp .env.example .env
docker compose up --build -d
```

This builds the Go binaries and starts all 11 containers/services. The short-lived `kafka-init` container creates the required Kafka topics, and `debezium-init` registers the source connector; both exit successfully after initialization.

### 2. Register the Debezium connector

```bash
curl -X PUT http://localhost:8083/connectors/platform-postgres/config \
  -H 'Content-Type: application/json' \
  --data @deployments/debezium/connector.json
```

### 3. Verify everything is running

```bash
docker compose ps
```

Expected output: the long-running services are "Up" or "Up (healthy)"; `kafka-init` and `debezium-init` are expected to be "Exited (0)" after initialization.

```bash
curl http://localhost:3000/api/health
# {"status":"ok"}

curl http://localhost:8083/connectors/platform-postgres/status
# {"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}
```

### 4. Open the dashboard

```text
http://localhost:3000
```

### 5. Create an order

Use the dashboard UI, or:

```bash
curl -X POST http://localhost:8080/orders \
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

### 6. Verify consumer processing

```bash
docker compose exec postgres psql -U platform -d platform -c "
  SELECT 'analytics' AS service, count(*) FROM analytics_order_events
  UNION ALL
  SELECT 'notifications', count(*) FROM notifications
  UNION ALL
  SELECT 'inventory', count(*) FROM inventory_reservations;
"
```

### 7. Verify Redis cache

```bash
docker compose exec redis redis-cli KEYS "inventory:order:*"
```

### 8. Inspect events in Kafka

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic platform.public.orders \
  --from-beginning \
  --max-messages 1
```

### Stop

```bash
docker compose down
```

---

## API Reference

### Order Service (`localhost:8080`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check with DB ping |
| `POST` | `/orders` | Create a new order |
| `GET` | `/events/latest` | Latest Kafka CDC event |

#### POST /orders

**Request:**

```json
{
  "customer_id": "string (required)",
  "items": [
    {
      "product_id": "string (required)",
      "quantity": "integer > 0 (required)",
      "unit_price_cents": "integer >= 0 (required)"
    }
  ]
}
```

**Response (201 Created):**

```json
{
  "id": "uuid",
  "customer_id": "string",
  "status": "PENDING",
  "total_cents": 3000,
  "items": [...],
  "created_at": "2026-08-08T12:00:00Z"
}
```

**Validation:**
- Customer ID must not be empty
- At least one item required
- Each item needs product_id, positive quantity, non-negative price
- Request body limited to 1 MB

### Debezium Connect REST API (`localhost:8083`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/connectors` | List all connectors |
| `PUT` | `/connectors/{name}/config` | Create or update connector |
| `GET` | `/connectors/{name}/status` | Get connector status |
| `POST` | `/connectors/{name}/restart` | Restart connector |

---

## Operations

### View logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f order-service
docker compose logs -f inventory-service
```

### Consumer logs

```bash
docker compose logs -f analytics-service inventory-service notification-service
```

### Connector recovery

```bash
# Check status
curl http://localhost:8083/connectors/platform-postgres/status

# Restart connector
curl -X POST http://localhost:8083/connectors/platform-postgres/restart

# Restart a specific task
curl -X POST http://localhost:8083/connectors/platform-postgres/tasks/0/restart
```

### Database inspection

```bash
# Interactive shell
docker compose exec postgres psql -U platform -d platform

# Table list
\dt

# Order counts
SELECT count(*) FROM orders;

# Consumer processing counts
SELECT consumer_name, count(*) FROM processed_events GROUP BY consumer_name;
```

### Clear everything and restart

```bash
docker compose down -v
docker compose up --build -d
```

### Apply migrations to an existing volume

If PostgreSQL was already running when schema changes were added:

```bash
docker compose exec -T postgres psql -U platform -d platform < migrations/001_init.sql
```

### Quick validation

```bash
make check
```

Runs: `gofmt`, `go vet`, `go test`, `docker compose config --quiet`.

---

## Production Deployment

A production-ready Compose example is available at `deployments/compose/docker-compose.prod.example.yml`.

### Production vs Development

| Feature | Development | Production |
|---|---|---|
| Build | `docker compose build` | Pre-built images in registry |
| Credentials | In Compose file | Secret manager or vault |
| Kafka | Single broker, KRaft | 3+ brokers, TLS, authentication |
| PostgreSQL | Single instance | Managed or replicated cluster |
| Redis | Single instance | Managed or cluster |
| Topics | Auto-created | Explicitly created with RF 3 |
| Networking | All ports exposed | Internal-only, reverse proxy only |
| Security | None | `read_only: true`, `no-new-privileges` |
| Database per service | Shared database | Separate databases or schemas |
| Event schema | Debezium raw | Schema Registry + versioned envelopes |
| Monitoring | Docker logs | Prometheus + Grafana + alerting |

### Production Checklist

- [ ] Use managed PostgreSQL with logical replication enabled
- [ ] Configure 3+ Kafka brokers with TLS and SASL authentication
- [ ] Use external secret manager (Vault, AWS Secrets Manager, etc.)
- [ ] Replace shared database with per-service databases or schemas
- [ ] Add a dedicated event relay to transform Debezium CDC into versioned application events
- [ ] Deploy Schema Registry for event compatibility enforcement
- [ ] Set explicit Kafka topic partition counts and replication factor 3
- [ ] Add retry topics and dead-letter queues per consumer
- [ ] Export Prometheus metrics: request rate, error rate, consumer lag, DLQ depth
- [ ] Configure alerts for connector failure, high consumer lag, DB connection exhaustion
- [ ] Run database migrations as an init container or release job, not at application start
- [ ] Use read-only root filesystems and non-root users
- [ ] Set CPU/memory resource limits and PodDisruptionBudgets
- [ ] Never expose PostgreSQL, Kafka, Redis, or Debezium ports externally
- [ ] Never commit `.env` files or credentials to version control

---

## Files in This Repository

| File/Directory | Purpose |
|---|---|
| `docker-compose.yml` | Local development stack |
| `Dockerfile` | Multi-stage Go build for both binaries |
| `go.mod` / `go.sum` | Go module and locked dependencies |
| `Makefile` | `up`, `down`, `logs`, `test`, `fmt`, `check` |
| `.env.example` | Environment variable template |
| `.gitignore` | Secrets, logs, IDE files, builds |
| `.dockerignore` | Excluded from Docker build context |
| `cmd/order-service/main.go` | Order-service composition root |
| `cmd/worker-service/main.go` | Worker-service composition root |
| `internal/config/*` | Environment-backed configuration |
| `internal/events/*` | Kafka observer and in-memory dashboard event store |
| `internal/order/*` | Order domain, business rules, HTTP adapter, PostgreSQL repository |
| `internal/platform/*` | PostgreSQL readiness and Redis client adapters |
| `internal/worker/*` | Debezium event parsing, projections, idempotency, worker pool |
| `migrations/001_init.sql` | Database schema |
| `frontend/*` | Operator dashboard |
| `deployments/debezium/*` | CDC connector configuration |
| `deployments/compose/*` | Production Compose template |
| `docs/architecture.md` | Production boundaries and rules |
| `docs/operations.md` | Runbook and recovery procedures |
| `README.md` | This file |

---

## Key Design Decisions

1. **Debezium, not application-level event publishing.** The Order Service does not manually produce Kafka events. Debezium reads the PostgreSQL WAL, eliminating dual-write consistency problems.

2. **Separate consumer groups, not a single consumer.** Inventory, Notification, and Analytics each have their own group. Kafka delivers the same event to each independently. One failing consumer does not block the others.

3. **At-least-once with idempotency, not exactly-once.** Kafka's exactly-once semantics require transactional producers, which Debezium does not support. The `processed_events` table provides idempotency at the consumer level instead.

4. **Commit after process, never before.** Offsets are committed only after successful business logic execution. If processing fails, Kafka redelivers the message.

5. **Shared worker binary, separate containers.** Inventory, Notification, and Analytics share `cmd/worker-service` code but run as separate Docker containers with different `WORKER_ROLE` environment variables. In production they would be independently versioned images.

6. **PostgreSQL as source of truth, Redis as cache.** PostgreSQL is the durable record. Redis caches reservation state but is not the system of record. If Redis loses data, it can be rebuilt from PostgreSQL.

7. **Nginx reverse proxy for the dashboard.** The dashboard runs on `localhost:3000` and proxies `/api/*` to the Order Service and `/connectors/*` to Debezium. The browser only connects to one origin, avoiding CORS.

8. **KRaft Kafka, no ZooKeeper.** Modern Kafka supports KRaft mode, which removes the ZooKeeper dependency. This simplifies the development stack.

---

## Future Improvements

- Add gRPC for synchronous inter-service communication (e.g., stock availability checks)
- Implement a dedicated **event relay** to transform raw Debezium CDC into versioned application events
- Add **retry topics** and **dead-letter queues** for failed event processing
- Add **Prometheus metrics** and **Grafana dashboards** for consumer lag and throughput
- Implement **API authentication and authorization** (JWT for REST, mTLS for services)
- Add **rate limiting** on the REST API
- Use **per-service PostgreSQL schemas** instead of a shared database
- Replace shared worker binary with independently built and versioned service images
- Add **integration tests** for the full `REST → PostgreSQL → Debezium → Kafka → Consumer` flow
- Add **Kubernetes manifests** for production orchestration
- Implement **Schema Registry** for event schema compatibility enforcement
- Add **circuit breakers** and **backpressure** on external service calls
