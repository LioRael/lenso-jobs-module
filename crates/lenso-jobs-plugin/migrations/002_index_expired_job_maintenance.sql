CREATE INDEX jobs_expired_exhausted_retirement
    ON jobs (queue, lease_expires_at, id)
    WHERE status = 'running' AND attempts >= max_attempts;

CREATE INDEX jobs_expired_retryable_claim
    ON jobs (queue, lease_expires_at, created_at, id)
    WHERE status = 'running' AND attempts < max_attempts;
