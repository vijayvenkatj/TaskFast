# Task Queue

A small, distrubuted task queue with HTTP endpoints for enqueuing and fetching tasks. It includes a scheduler for delayed execution and a lease-based reaper to handle worker timeouts and retries.

## Architecture (simple)
```task-queue/README.md#L5-23
+------------------+        +--------------------+
|  HTTP API (api)  | -----> |   Engine (core)    |
|  /api/* routes   |        |  queues + leases   |
+------------------+        +--------------------+
                                  |    |    |
                                  |    |    +--> DLQ (dead tasks)
                                  |    +------> Processing (leases)
                                  +-----------> Scheduled -> Ready
```

### Key components
- **API layer** (`internal/api`) handles JSON requests and forwards to the engine.
- **Engine** (`internal/engine`) manages queues and task state in memory.
- **Scheduler** moves due tasks from the scheduled min-heap into the ready queue.
- **Reaper** expires leases and retries tasks using exponential backoff.

### Task flow
1. **Enqueue**: task is pushed into the scheduled heap.
2. **Scheduler**: when `RunAt` is due, task moves to the ready queue.
3. **Fetch**: worker receives a task and a lease is created.
4. **Ack/Fail**: 
   - `Ack` marks task completed.
   - `Fail` retries with backoff or moves to DLQ after `MaxRetries`.

## API
- `POST /api/enqueue`
  - Body: `{ "task": { "id": 1, "payload": "...", "run_at": "...", "max_retries": 3, "retries": 0 } }`
  - Response: `{ "message": "task enqueued" }`
- `POST /api/fetch`
  - Body: `{ "worker_id": 123, "task_time": "1s" }`
  - Response: `{ "task": { ... } }`
- `GET /api/dlq`
  - Response: `{ "dead_tasks": [ ... ] }`
- **Planned**: `POST /api/ack`
  - Body: `{ "task": { "id": 1 } }`
  - Response: `{ "message": "acknowledged" }`
- **Planned**: `POST /api/fail`
  - Body: `{ "task": { "id": 1 }, "error": "..." }`
  - Response: `{ "message": "failed" }`

## Run locally
```task-queue/README.md#L52-56
go run ./cmd
```
The server listens on `:8080`.

## Project structure
- `cmd/main.go` — entrypoint
- `internal/api` — HTTP handlers, router, server
- `internal/engine` — in-memory queues, scheduler, reaper
- `internal/storage` — reserved for persistence (currently empty)

## Upcoming
- Persistent storage for tasks and state.
- Raft-based durability and replication.
