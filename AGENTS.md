# Agent instructions

This repository contains the first-party durable Jobs Module for Lenso.

- Jobs owns durable scheduling, leases, retry policy, terminal state, and operational evidence.
- Business Modules own job payload meaning, handler behavior, and idempotency of external effects.
- Keep PostgreSQL behind the Module boundary. Runtime code verifies an operator-managed schema and never creates or migrates it during App boot.
- Kernel tasks remain volatile and product-neutral; do not add Jobs policy to Kernel, Drivers, or Adapters.
- PostgreSQL acceptance tests require `LENSO_JOBS_TEST_DATABASE_URL` and must target a database whose name starts with `lenso_jobs_test`.
- Registry publication, immutable tags, GitHub Releases, and remote repository creation require explicit approval.
