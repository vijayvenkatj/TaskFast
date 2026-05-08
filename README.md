# Task Queue

A deterministic, event-sourced distributed task queue with a write-ahead log (WAL), replay-based recovery, and a clean separation between runtime orchestration and a durable state machine.

## Architecture
```task-queue/README.md#L7-23
+--------------------+        +-------------------------+
|  HTTP API (api)    | -----> |  Runtime Orchestration  |
|  /api/* routes     |        |  queues + leases        |
+--------------------+        +-----------+-------------+
                                        |
                                        v
                               +-----------------+
                               |   WAL Append    |
                               +-----------------+
                                        |
                                        v
                               +-----------------+
                               |  Apply(event)   |
                               |  State Machine  |
                               +-----------------+
```

### Durable state (authoritative)
The only durable source of truth is:

```task-queue/README.md#L27-36
tasks map[taskID]*TaskMeta

TaskMeta:
- Task
- Status (READY | DELAYED | PROCESSING | DLQ)
- Retries / MaxRetries
- Lease
```

### Runtime indexes (rebuildable)
These are materialized projections derived from `tasks` during recovery:
- ready queue
- scheduled min-heap
- processing map (leases)
- dlq queue

### Deterministic event flow
Runtime logic performs orchestration and emits deterministic events:

```task-queue/README.md#L45-49
Runtime logic
  -> Append WAL
  -> Apply(event)
  -> Update runtime indexes
```

`Apply(event)` **only** mutates the canonical `tasks` map and never performs time-based decisions, queue operations, or scheduling.

### Recovery
`Restore()` implements:
1. WAL replay
2. Apply each event
3. Normalize invalid ownership (e.g., PROCESSING -> READY, expired DELAYED -> READY)
4. Rebuild runtime indexes from `tasks`

## Task lifecycle
- **Enqueue**: runtime decides READY vs DELAYED, appends `EnqueueEvent`, applies it, then queues the task.
- **Fetch**: runtime pops from ready, creates lease expiry, appends `FetchEvent`, applies it, then tracks processing lease.
- **Fail**: runtime computes backoff and DLQ decision, appends `FailEvent`, applies it, then re-queues or DLQs.
- **Ack**: runtime appends `AckEvent`, applies it, then removes task from processing.

## API
### `POST /api/enqueue`
Body:
```task-queue/README.md#L70-76
{
  "task": { "id": 1, "payload": "...", "run_at": "2025-01-01T00:00:00Z" }
}
```
Response:
```task-queue/README.md#L78-80
{ "message": "task enqueued" }
```

### `POST /api/fetch`
Body:
```task-queue/README.md#L84-89
{
  "worker_id": 123,
  "task_time": "1s"
}
```
Response:
```task-queue/README.md#L91-93
{ "task": { "id": 1, "payload": "...", "run_at": "2025-01-01T00:00:00Z" } }
```

### `POST /api/ack`
Body:
```task-queue/README.md#L97-99
{ "task_id": 1 }
```
Response:
```task-queue/README.md#L101-103
{ "message": "task acknowledged" }
```

### `POST /api/fail`
Body:
```task-queue/README.md#L107-110
{ "task_id": 1, "error": "worker crashed" }
```
Response:
```task-queue/README.md#L112-114
{ "message": "task failed" }
```

### `GET /api/dlq`
Response:
```task-queue/README.md#L118-120
{ "dead_tasks": [ ... ] }
```

## Run locally
```task-queue/README.md#L124-126
go run ./cmd
```
The server listens on `:8080`.

## Project structure
- `cmd/main.go` — entrypoint
- `internal/api` — HTTP handlers, router, server
- `internal/engine` — runtime orchestration, scheduler, reaper, event-sourced apply
- `internal/model` — durable task and event definitions
- `internal/storage` — WAL append/replay

## Notes
- `MaxRetries` defaults to `1` in the current engine configuration.
- WAL files are required for replay and recovery; keep them persistent if you want durability across restarts.
