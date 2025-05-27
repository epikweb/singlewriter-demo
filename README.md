
# 🧠 Event-Sourced API Server

A fully integrated event-sourced API server using a log-structured store (LSS), functional core, and post-commit async effects. This system guarantees deterministic behavior, rollback-safe transactions, and clean separation between pure state and side effects.

---

## 🚀 Architecture Overview

This system is built around:

- **Functional Core**: Pure reducers for deterministic state transitions
- **Log-Structured Storage (LSS)**: Durable append-only store with total ordering
- **Post-Commit Triggers**: Async side effects (like emails) that run only after durable write
- **Atomic Execution Model**: All operations are serialized with a single-writer mutex

---

## 🧩 Components

### 📦 Log-Structured Storage (LSS)

- Append-only table with physical order (`lss_order_id`)
- Sharded by `lss_partition_id`
- Guaranteed write durability before any external effects are triggered

#### Schema
```sql
CREATE TABLE lss (
  lss_order_id BIGINT,
  lss_partition_id VARCHAR,
  lss_type VARCHAR,
  lss_data JSONB,
  lss_metadata JSONB
);
```

---

### 🔧 Functional Core

#### `StateChange`
Defines state machines and event mappers for commands.

```js
{
  viewId: 'Subscription.Create',
  map: (command, state) => [ { type: 'Subscription.Created', ... } ],
  reduce: { 'Subscription.Created': (state, event) => updatedState }
}
```

#### `StateView`
Projects state from events.

```js
{
  viewId: 'Subscription.List',
  reduce: {
    'Subscription.Created': (state, data) => newState,
    ...
  }
}
```

#### `StateMachine`
Reacts to view changes and issues new commands.

```js
{
  viewId: 'Assignment.Tracker',
  trigger: ({ query, produce }) => {
    ...
    produce({ type: 'Subscription.Assign.Members', ... })
  }
}
```

---

## 🔒 Atomicity Between Core + LSS

### Step-by-step Flow
1. HTTP handler calls `core.produce(...)`
2. `core.reduce()` applies events to views
3. `core.commit()` returns the full event list
4. `lss.physicalAppend()` persists to disk
5. Only after successful write do async processors (e.g., email) fire

### Single Writer Mutex
Prevents concurrency issues by serializing access to:
- Event generation
- State mutation
- LSS writes

### Rollback Safety
If any error occurs before persistence:
- `core.rollback()` restores the previous snapshot
- Transaction is discarded

---

## 📬 External Effects

All side effects are triggered **only after events are durably written**.

### Example: SendGrid Integration

- Email send attempt made via HTTP after LSS append
- Success: emits `Email.Succeeded`
- Failure: emits `Email.Failed`

### Retry Strategy

- `Emails.To.Send` view tracks notification attempts
- `Email.Failed` reducer increments `.attempt`
- After 10 attempts, the message is dropped

---

## 🧪 Built-in Testing

```bash
NODE_ENV=test node server.js
```

### Tests run:
- `core.produce()` emits correct event
- View projection (`Subscription.List`) matches expected result

---

## 🔌 HTTP API

### POST `/create-subscription`
Creates a new subscription.

Request:
```json
{
  "plan": "gold",
  "createdBy": "admin@site.com"
}
```

Response:
```json
{
  "ok": true,
  "subscriptionId": "sub-1"
}
```

---

### GET `/subscriptions/:id`
Fetches current view state for a subscription.

---

## 🧱 Recovery Process

On server boot:
```js
const history = await lss.sharedReader.physicalRead()
history.forEach(core.reduce)
```

- All events are replayed
- Full state is rebuilt deterministically
- No snapshots required

---

## ✅ Key Guarantees

- 🧠 Deterministic state from event history
- 💾 Durable and ordered LSS writes
- 🔄 Retry-safe post-commit async processors
- 🔒 Serializable single-writer safety
- 🧪 CI-friendly test harness

---

## 📁 Deployment

No external dependencies beyond:

- PostgreSQL
- Node.js
- Optional: SendGrid API key (`SENDGRID_API_KEY`)

---

## 📬 Contact

Questions or contributions welcome. Open a PR or issue.
