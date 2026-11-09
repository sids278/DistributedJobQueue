# Project Status

Last updated: 2026-10-26

## At a glance

This workspace currently contains two related but separate pieces:

1. An in-memory job repository, read-through cache, and SQL-job experiment in `Jobs/`.
2. A Redis-backed distributed queue implementation under `_reference/`.

The root program in `main.go` currently demonstrates the first piece. It does
not start or use the Redis queue.

## What the root program does

Run it with:

```bash
go run .
```

`main.go` creates a `MemoryJobRepository` backed by a Go slice, wraps it in a
`CachedJobRepository`, stores a sample `Job` containing `SELECT 1`, fetches it
through the cache, and prints its ID and query. The cache is assigned to a
`Jobs.JobResps` interface variable, so the example also shows the interface in
use.

The in-memory repository in [`Jobs/memory_repository.go`](./Jobs/memory_repository.go)
supports create, read, list, update, and delete operations. It assigns IDs,
protects its slice for concurrent access, and returns copies of stored jobs.
The cache in [`Jobs/cached_repository.go`](./Jobs/cached_repository.go) caches
job lookups by ID. Writes made through the cache update or invalidate its
entries. Both storage and cache are process-local and are lost when the
program exits.

## SQL execution status

SQL execution is **not implemented yet**. `MemoryJobRepository.RunJob` looks up
the job and calls `RunSqlJob`, but `RunSqlJob` returns
`ErrSQLNotImplemented` for a non-empty query. It does not connect to a
database or execute SQL.

[`Jobs/SqlJob.go`](./Jobs/SqlJob.go) defines `ScheduledSqlJob` with a `Run`
method. This type is currently separate from the repository's `Job` type and is
not used by `main.go` or stored by `MemoryJobRepository`. Scheduling fields
(`Schedule` and `TimeInterval`) are data only; no scheduler processes them.

## Current interface and implementation

[`Jobs/job.go`](./Jobs/job.go) defines:

- `Job`: the stored record (`Id`, `Desc`, and `Query`).
- `JobResps`: the repository-style contract for CRUD operations and running a
  job.

`MemoryJobRepository` implements `JobResps`. The `isSchedulable` method is
unexported, so implementations outside the `Jobs` package cannot implement
this interface as currently written. If other packages need to provide
repositories, consider exporting or removing that method.

## Distributed queue status

The README describes an at-least-once Redis Streams queue with workers,
retries, backoff, and a dead-letter queue. The queue code and CLI visible in
this workspace are under
[`_reference/queue/`](./_reference/queue/) and
[`_reference/cmd/jobqueue/`](./_reference/cmd/jobqueue/), rather than at the
root paths shown in the README's layout. The root `main.go` does not wire these
components to `Jobs` yet.

## Tests and build

The `Jobs` package has tests for slice-backed CRUD behavior and for the
explicit “SQL executor not configured” result in
[`Jobs/memory_repository_test.go`](./Jobs/memory_repository_test.go), as well as
cache reads and cache consistency in
[`Jobs/cached_repository_test.go`](./Jobs/cached_repository_test.go).

Run the Go tests and build from the repository root:

```bash
go test ./...
go build ./...
```

The README also documents `make test` and `make test-redis` for the queue. Redis
integration testing requires a reachable Redis server.

## Suggested next steps

1. Decide whether the current goal is a standalone SQL job repository or a
   worker-backed distributed queue; clarify how these pieces should connect.
2. If continuing with the SQL repository, define a storage interface and keep
   the in-memory implementation as one backend.
3. Add a SQL execution abstraction, then select and configure a database
   driver before implementing actual query execution.
4. Decide how scheduled jobs are represented and triggerable before adding a
   scheduler.
5. If the Redis queue is part of the active application, move or wire its
   packages into the application path and update the README layout and entry
   point accordingly.
