# 🧠 Single Writer Demo: Event-Sourced Subscription API

This project implements an event-sourced backend for managing subscriptions using a **log-structured storage (LSS)** model with a **single-writer/shared-reader** architecture.

## 🌩 Architecture Overview

- **Functional Core**: Pure logic, driven by event folding and query projection.
- **LSS (Log-Structured Storage)**: Append-only physical log backed by PostgreSQL with guaranteed ordering (`lss_order_id`) and partitioning (`lss_partition_id`).
- **State Machines**: Reactive layer observing projections and dispatching new commands.
- **Shared Reader**: For reads, projections, and history folding during startup or snapshot generation.

## 📦 Tables

The `lss` table schema:
```sql
CREATE TABLE lss (
  lss_order_id     BIGINT       NOT NULL,
  lss_partition_id VARCHAR(255) NOT NULL,
  lss_type         VARCHAR(255) NOT NULL,
  lss_data         JSONB        NOT NULL,
  lss_metadata     JSONB        NOT NULL
);
```

Bootstrap:
```sql
INSERT INTO lss (lss_order_id, lss_partition_id, lss_type, lss_data, lss_metadata)
VALUES (0, 'system', 'LSS.Initialized', '{}', '{}');
```

## 🚦 Recovery

On startup, the system folds all historical events into the functional core using:
```js
const history = await lss.sharedReader.physicalRead();
history.forEach(core.reduce);
```

## 📤 API

- `POST /create-subscription` – Create a new subscription with `plan` and `createdBy`.
- `GET /subscriptions/:id` – Query current state of a subscription via its ID.

## 🧪 Tests

To run internal tests:
```bash
NODE_ENV=test node app.js
```

## 🧬 Event Model

This system is modeled according to this event model diagram:

![Event Model](EventModel.png)

## 🐘 Database

Ensure PostgreSQL is available at:

```bash
postgres://postgres:postgres@localhost:5432/postgres
```

You can use the provided `docker-compose.yml` to spin up a database locally.

## 🔁 GitHub Actions

CI runs tests in `NODE_ENV=test` mode and includes a PostgreSQL service.

---
© 2025 epikweb. MIT License.
