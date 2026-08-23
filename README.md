# Lenso Jobs Module

`lenso-jobs-module` is the first-party durable single-step Jobs Module for Lenso applications.

It owns:

- durable enqueue and scheduled availability;
- an explicit bounded queue set per keyed Module Instance;
- caller-scoped idempotency keys;
- fenced, expiring worker leases;
- bounded retry and terminal failure policy;
- success, failure, and inspection evidence; and
- an operator-managed PostgreSQL schema.

It deliberately does not own business payload meaning, handler implementation, external-effect idempotency, multi-step workflow orchestration, or Kernel scheduling. Removing the Module removes the job records, leases, retry policy, and operational surface without changing Kernel.

## First tracer slice

One authorized producer enqueues a typed job. One authorized worker claims it under an opaque lease and either completes it or reports a retryable/non-retryable failure. An expired lease can never complete a job and may be safely reclaimed with a new fencing token. An observer can inspect the durable state.

The portable `lenso.jobs@1` Capability provides:

- `enqueue`
- `claim`
- `renew`
- `complete`
- `fail`
- `inspect`

Workflow graphs, recurring schedules, priorities, cancellation, progress streams, and a Web/Console surface are intentionally deferred until a real consumer requires them.

## Ownership

The Jobs Module owns job identity, queue placement, availability time, attempt count, lease generation, lease expiry, retry schedule, terminal status, and the last stable failure code. A consuming business Module owns the schema and meaning of `payload`, selects the job kind, and makes every external effect idempotent because execution is at-least-once.

Each keyed Jobs Instance declares its allowed queues and caller Instances. Use separate Jobs Instances when queues cross trust or operational boundaries.

PostgreSQL is a private persistence Adapter. The Module uses `lenso-postgres-kit` to verify its schema during `prepare`; setup and upgrades are explicit operator workflows.

One Instance uses immutable configuration validated again by the factory before preparation:

```json
{
  "schema": "jobs_email",
  "database_url_secret": "jobs/database-url",
  "lease_seconds": 30,
  "retry_base_seconds": 5,
  "retry_max_seconds": 300,
  "queues": ["email"],
  "producer_instances": ["accounts", "organization"],
  "worker_instances": ["email-worker"],
  "observer_instances": ["operations"]
}
```

The schema is [`crates/lenso-jobs-module/config.schema.json`](crates/lenso-jobs-module/config.schema.json). The database URL itself remains behind the explicitly bound Secrets Capability.

## Development

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

PostgreSQL acceptance additionally requires a disposable database whose name starts with `lenso_jobs_test`:

```sh
LENSO_JOBS_TEST_DATABASE_URL=postgres://... \
  cargo test --locked --workspace --features postgres-acceptance
```
