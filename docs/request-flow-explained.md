# Request Flow & Project Anatomy — Explained

A conversational deep-dive into what this project actually does, file by file, following one `POST /orders` request from the browser all the way through to the database projections.

---

## 1. The Big Picture

This platform turns a database insert into three independent downstream reactions. It does **not** use application-level event publishing. Instead:

1. A REST API persists an order in PostgreSQL.
2. Debezium (Change Data Capture) watches PostgreSQL's Write-Ahead Log (WAL) and notices the insert.
3. Debezium publishes the change as a JSON event to Kafka.
4. Three independent Go consumer services (Inventory, Notification, Analytics) each read that event from their own consumer group and write their own projection.

```
Browser → Nginx → Order Service → PostgreSQL → (WAL) → Debezium → Kafka → 3 Worker Services → PostgreSQL + Redis
```

---

## 2. Three Separate Things: Code, Servers, CDC

| Layer | What it is | Examples |
|---|---|---|
| **Code** | Static source files that do nothing by themselves | `cmd/`, `internal/`, `deployments/debezium/connector.json` |
| **Your servers** | Running processes built from your code | order-service, inventory/notification/analytics workers, Nginx |
| **CDC service** | A *separate* Java server running Debezium's own code (not yours) | Debezium Connect container |

They communicate, but none contains the others:

```
YOUR GO SERVERS ──write──► PostgreSQL ──watch──► DEBEZIUM (Java) ──publish──► Kafka ──consume──► YOUR GO WORKERS
```

Your code never tells Debezium anything. It just watches the database itself.

---

## 3. File Structure (Visual Map)

```
Real_Time_Data_Integration_Platform/
│
├── .dockerignore              # Files excluded from Docker build context
├── .env.example               # Template for env vars (copy → .env)
├── .gitignore                 # Git-ignored files (secrets, logs, builds)
├── CLAUDE.md                  # AI-agent instructions (vault doc workflow)
├── Dockerfile                 # Multi-stage Go build → runtime image
├── Makefile                   # Shortcuts: up, down, logs, test, fmt, check
├── README.md                  # Full project docs, architecture, runbook
├── docker-compose.yml         # 11-service local dev stack definition
├── go.mod / go.sum            # Go module + locked dependencies
│
├── cmd/                       # Entry points (composition roots)
│   ├── order-service/
│   │   └── main.go            # Boots the REST API server (POST /orders, /health, /events/latest)
│   └── worker-service/
│       └── main.go            # Boots the Kafka consumer; WORKER_ROLE picks Inventory/Notification/Analytics
│
├── internal/                  # Internal Go packages (not importable outside repo)
│   ├── config/
│   │   └── config.go          # Reads env vars into a Config struct
│   │
│   ├── events/
│   │   └── observer.go        # Watches Kafka, stores latest event for dashboard live CDC view
│   │
│   ├── order/                 # Order-service domain
│   │   ├── domain.go          # Order, OrderItem structs + validation/business rules
│   │   ├── ports.go           # Interfaces (repository, service) for clean architecture
│   │   ├── service.go         # Use-cases: validate, compute total, persist order
│   │   ├── http_handler.go    # HTTP handlers (REST adapter): create order, health
│   │   ├── postgres_repository.go  # Postgres implementation of order repository
│   │   └── service_test.go    # Unit tests for order service logic
│   │
│   ├── platform/              # Infrastructure adapters
│   │   ├── postgres.go        # Postgres connection pool + readiness ping
│   │   └── redis.go           # Redis client setup
│   │
│   └── worker/                # Kafka consumer logic
│       ├── consumer.go        # Kafka reader + worker-pool (4 goroutines, buffered channel, offset commit)
│       ├── event.go           # Debezium CDC JSON envelope model + parsing
│       ├── handler.go         # Per-role business logic (inventory/notification/analytics)
│       └── event_test.go      # Tests for event parsing
│
├── migrations/
│   └── 001_init.sql           # Schema: orders, order_items, processed_events, notifications, analytics_order_events, inventory_reservations
│
├── frontend/                  # Operator dashboard (served by Nginx)
│   ├── index.html             # Dashboard page markup
│   ├── styles.css             # Responsive styling (mobile + desktop)
│   ├── app.js                 # Order creation form + live CDC event viewer
│   └── nginx.conf             # Reverse proxy: /api/* → order-service, /connectors/* → Debezium
│
├── deployments/               # Deployment configs
│   ├── debezium/
│   │   ├── connector.json     # Postgres CDC connector config (WAL source)
│   │   └── register-connector.sh  # Registers the connector with Debezium Connect
│   └── compose/
│       └── docker-compose.prod.example.yml  # Production deployment template (read-only, secrets, etc.)
│
└── docs/                      # Technical guides
    ├── architecture.md        # Architecture, layers, production boundaries
    ├── event-reference.md     # Debezium schema/payload/op-codes reference
    ├── operations.md          # Runbook: health, logs, troubleshooting, reset
    └── request-flow-explained.md  # This file
```

---

## 4. Every File's Role in One Request Flow

### Stage 1 — Request arrives at the dashboard
- **`frontend/nginx.conf`** — Nginx serves the static dashboard and reverse-proxies: `/api/*` → order-service:8080, `/connectors/*` → debezium:8083. Keeps everything same-origin, no CORS.
- **`frontend/index.html` / `app.js` / `styles.css`** — the UI. `app.js` builds the JSON body, calls `/api/orders`, renders the 201 response, and polls `GET /events/latest` to show the CDC event live.

### Stage 2 — Order service boot (once per container start)
- **`cmd/order-service/main.go`** — composition root. Loads config, creates the DB pool, waits for Postgres, wires repository → service → handler, starts the dashboard observer goroutine, then starts the HTTP server with graceful shutdown.
- **`internal/config/config.go`** — `LoadOrder()` reads `DATABASE_URL`, `ORDER_SERVICE_PORT`, `KAFKA_BROKERS` from env with defaults.
- **`internal/platform/postgres.go`** — `NewPostgresPool` (lazy connection pool) and `WaitForPostgres` (pings the pool 30×1s so the service never serves before the DB is ready).

### Stage 3 — HTTP handling
- **`internal/order/http_handler.go`** — REST adapter. `Routes()` registers `POST /orders`, `GET /health`, `GET /events/latest`. `create()` limits the body to 1MB, rejects unknown fields and trailing JSON, maps `ValidationError` → 400, other errors → 500, success → 201.

### Stage 4 — Business rules
- **`internal/order/domain.go`** — pure data types: `CreateOrderRequest`, `OrderItem`, `OrderDraft`, `Response`, `ValidationError`. Money in cents. No logic.
- **`internal/order/service.go`** — `Create()` validates (customer_id, ≥1 item, quantity > 0, price ≥ 0, overflow checks), generates `uuid.New()`, and hands an `OrderDraft` to the repository.
- **`internal/order/ports.go`** — the `Repository` interface, the seam that keeps business logic independent of Postgres.

### Stage 5 — Persistence (the transaction)
- **`internal/order/postgres_repository.go`** — `Create()` opens **one transaction**: inserts the `orders` row (RETURNING created_at) and every `order_items` row, then commits. Any failure rolls everything back — no order without items, no items without an order.

### Stage 6 — CDC (no app code, but config matters)
- **`migrations/001_init.sql`** — created all tables. The commit is what lands in the WAL.
- **`deployments/debezium/connector.json`** — tells Debezium which tables to watch, which plugin (`pgoutput`), topic naming, and the replication slot `platform_slot`.
- **`deployments/debezium/register-connector.sh`** — helper that PUTs that config to Debezium Connect (`:8083`). Run once by the `debezium-init` container.

### Stage 7 — Workers consume the event
- **`cmd/worker-service/main.go`** — worker composition root, deployed 3× with different `WORKER_ROLE`. Builds the DB pool, waits for Postgres, creates Redis **only for the inventory role**, wires handler + consumer, and runs.
- **`internal/worker/consumer.go`** — the worker pool. A Kafka reader (group `{role}-service`, `FirstOffset`) fetches messages into a **buffered channel**; N goroutines process them. **Offset is committed only on success** — on error it logs and skips so Kafka redelivers.
- **`internal/worker/event.go`** — Debezium JSON model (`after.id`, `source.table/lsn`, `op`). `ID()` builds the idempotency key: `{table}:{lsn}` or `{table}:{id}:{op}`.
- **`internal/worker/handler.go`** — role projection: skips deletes/null-after, opens a transaction, `INSERT ... ON CONFLICT DO NOTHING` into `processed_events` (the idempotency claim, same transaction as the projection — failure rolls back the claim so Kafka can retry), then writes the role-specific table, commits, and for inventory sets Redis `inventory:order:{id}=RESERVED` (TTL 24h).

### Stage 8 — Dashboard live view
- **`internal/events/observer.go`** — the `Observer` runs inside the order-service process, uses its own consumer group `dashboard-observer` starting at `LastOffset` (new events only), and stores the raw JSON in an in-memory `Store` (mutex-guarded, returns copies).
- Back in **`internal/order/http_handler.go`** — `latestEvent()` reads that store and returns it to `app.js`, which renders it.

### Support files
- **`Dockerfile`** — multi-stage build for both binaries.
- **`Makefile`** — `up/down/logs/test/fmt/check`.
- **`docker-compose.yml`** — the 11 containers incl. `kafka-init` (topic creation) and `debezium-init` (connector registration).
- **`deployments/compose/docker-compose.prod.example.yml`** — production template (read-only FS, secrets, image-based).
- **`docs/*`** — docs only, no runtime role.
- **`internal/order/service_test.go`, `internal/worker/event_test.go`** — unit tests.

---

## 5. Key Concepts Clarified

### What is a transaction?
A transaction is a group of database operations that must all succeed or all fail together (ACID). In `postgres_repository.go:21`, `Begin()` starts one; the `orders` insert and all `order_items` inserts happen inside it, then `Commit()`. If any item insert fails, `Rollback()` undoes the header too. The worker uses the same pattern for the idempotency claim + projection.

### Where are Kafka topics created?
In **Docker, not code** — the `kafka-init` container (`docker-compose.yml:46`) runs `kafka-topics.sh --create` for all 6 topics before anything starts: the 3 Connect internal topics (compacted), `schemahistory.platform`, and `platform.public.orders` / `platform.public.order_items`. Go code only *uses* the topics by name (`consumer.go:24`, `observer.go:52`); it never creates them.

### Do we need to ping the DB pool? 
Yes — in three layers:
1. **Container healthchecks** (`docker-compose.yml`): `pg_isready`, Kafka API-versions, `redis-cli ping`.
2. **Startup readiness** (`platform.WaitForPostgres`): pings the pool before serving. Needed because `pgxpool.New` is lazy — it doesn't dial the DB until the first query.
3. **Runtime liveness** (`/health` endpoint): `db.Ping()` on every call, returning 503 if unreachable.

---

## 6. The Single Request Cheat-Sheet

```
POST /orders
  nginx.conf → order-service/main.go → config.go → postgres.go
  → http_handler.go → domain.go → service.go → ports.go → postgres_repository.go
  → PostgreSQL COMMIT (WAL)
  → connector.json → Debezium → Kafka (platform.public.orders)
  → consumer.go → event.go → handler.go → processed_events + projections + Redis
  → observer.go → http_handler.go → app.js (dashboard)
```