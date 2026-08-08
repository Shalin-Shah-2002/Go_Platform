# Event Reference

This guide explains the raw event produced by Debezium and how the current workers consume it.

## Raw Debezium Event

For an order insert, the event has two main parts:

```json
{
  "schema": { "...": "description of the event shape" },
  "payload": { "...": "actual change data" }
}
```

### `schema`

`schema` describes the structure of the message. It says that `after.id` is a string UUID, `total_cents` is an integer, and `created_at` is a timestamp.

It is metadata about the message, not the order itself.

### `payload`

`payload` contains the actual database change:

```json
{
  "before": null,
  "after": {
    "id": "7a7c19da-3178-4d65-813f-d24cbbcb404b",
    "customer_id": "cust-101",
    "status": "PENDING",
    "total_cents": 3000,
    "created_at": "2026-08-08T15:26:34.471170Z"
  },
  "source": {
    "db": "platform",
    "schema": "public",
    "table": "orders",
    "lsn": 27019416
  },
  "op": "c"
}
```

## Payload Fields

| Field | Meaning |
|---|---|
| `before` | Row before the change; `null` for an insert |
| `after` | Row after the change; contains the inserted/updated row |
| `source.db` | PostgreSQL database name |
| `source.schema` | PostgreSQL schema, usually `public` |
| `source.table` | Table that changed |
| `source.lsn` | PostgreSQL log sequence number for the change |
| `op` | Operation code |
| `transaction` | Transaction metadata when Debezium provides it |
| `ts_ms` | Event timestamp in milliseconds |

## Operation Codes

| Code | Meaning | `before` | `after` |
|---|---|---|---|
| `c` | Create / INSERT | `null` | New row |
| `u` | Update / UPDATE | Old row | New row |
| `d` | Delete / DELETE | Deleted row | Usually `null` |
| `r` | Read during snapshot | `null` or previous | Existing row |

Example create event:

```json
{
  "before": null,
  "after": {"id": "order-1", "status": "PENDING"},
  "op": "c"
}
```

Example update event:

```json
{
  "before": {"id": "order-1", "status": "PENDING"},
  "after": {"id": "order-1", "status": "CONFIRMED"},
  "op": "u"
}
```

## Topic Mapping

Debezium uses the `topic.prefix` value `platform` and the PostgreSQL schema/table name:

```text
topic = {prefix}.{schema}.{table}
```

Current mapping:

```text
public.orders
    -> platform.public.orders

public.order_items
    -> platform.public.order_items
```

This mapping is configured in:

```text
```

## How Workers Read the Event

The current worker model reads:

```text
platform.public.orders
```

The worker decodes only the fields it needs:

```go
type Event struct {
    Payload struct {
        After *struct {
            ID         string `json:"id"`
            CustomerID string `json:"customer_id"`
            TotalCents int64  `json:"total_cents"`
        } `json:"after"`
        Source struct {
            Table string `json:"table"`
            LSN   int64  `json:"lsn"`
        } `json:"source"`
        Op string `json:"op"`
    } `json:"payload"`
}
```

Workers do not need to model every field in the Debezium schema.

## Event ID and Idempotency

Kafka can deliver an event more than once. The worker creates a stable event ID:

```text
source.table + ":" + source.lsn
```

Example:

```text
orders:27019416
```

It records that ID with the consumer name:

```text
inventory     + orders:27019416
notification  + orders:27019416
analytics     + orders:27019416
```

The database primary key prevents the same service from applying the same change twice.

The event claim and service projection are in one PostgreSQL transaction:

```text
begin transaction
  insert processed_events
  insert service projection
commit transaction
```

If the projection fails, the transaction rolls back and Kafka can redeliver the event.

## Current Order Item Limitation

One order request writes two tables:

```text
orders       -> platform.public.orders
order_items  -> platform.public.order_items
```

The current workers consume only `platform.public.orders`. Therefore, the current inventory demonstration reserves at the order level and does not yet calculate stock per product/quantity from `order_items`.

## Recommended Application Event

Raw Debezium events are infrastructure events. A production system should transform them into a business event:

```json
{
  "event_id": "orders:27019416",
  "event_type": "order.created",
  "schema_version": 1,
  "aggregate_type": "order",
  "aggregate_id": "7a7c19da-3178-4d65-813f-d24cbbcb404b",
  "occurred_at": "2026-08-08T15:26:34.471170Z",
  "payload": {
    "customer_id": "cust-101",
    "items": [
      {
        "product_id": "prod-1",
        "quantity": 2,
        "unit_price_cents": 1500
      }
    ],
    "total_cents": 3000
  }
}
```

Then consumers depend on `order.created`, not on Debezium's vendor-specific envelope:

```text
Debezium raw topics
    -> CDC/event relay
    -> order.created application topic
    -> Inventory / Notification / Analytics
```
