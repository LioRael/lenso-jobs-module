CREATE TABLE jobs (
    id text PRIMARY KEY,
    producer_instance text NOT NULL,
    queue text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    max_attempts integer NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL,
    lease_generation bigint NOT NULL DEFAULT 0,
    lease_owner text,
    lease_token_hash bytea,
    lease_expires_at timestamptz,
    last_failure_code text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT jobs_status_valid CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    CONSTRAINT jobs_max_attempts_valid CHECK (max_attempts BETWEEN 1 AND 100),
    CONSTRAINT jobs_attempts_valid CHECK (attempts >= 0 AND attempts <= max_attempts),
    CONSTRAINT jobs_lease_shape_valid CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_token_hash IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'running' AND lease_owner IS NULL AND lease_token_hash IS NULL AND lease_expires_at IS NULL)
    ),
    CONSTRAINT jobs_producer_idempotency_unique UNIQUE (producer_instance, idempotency_key)
);

CREATE INDEX jobs_claim_order
    ON jobs (queue, available_at, created_at, id)
    WHERE status IN ('queued', 'running');
