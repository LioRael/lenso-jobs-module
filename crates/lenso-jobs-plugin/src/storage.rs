use std::collections::BTreeMap;

use lenso_capability_jobs::{
    ClaimResponse, FailResponse, FailResponseStatus, InspectResponse, InspectResponseStatus,
};
use lenso_postgres_kit::OwnedPostgres;
use serde_json::{Map, Value};
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;

use crate::{JobsError, format_time};

pub(crate) const EXPIRED_RETIREMENT_BATCH_LIMIT: i64 = 64;

#[derive(Clone, Debug)]
pub(crate) struct NewJob {
    pub id: String,
    pub producer: String,
    pub queue: String,
    pub kind: String,
    pub payload: BTreeMap<String, Value>,
    pub idempotency_key: String,
    pub available_at: OffsetDateTime,
    pub max_attempts: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EnqueueOutcome {
    Created(String),
    Existing(String),
    Conflict,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    pub base_seconds: i64,
    pub max_seconds: i64,
}

pub(crate) async fn enqueue(
    postgres: &OwnedPostgres,
    job: NewJob,
) -> Result<EnqueueOutcome, JobsError> {
    let mut transaction = postgres.pool().begin().await.map_err(db("begin enqueue"))?;
    let payload = Value::Object(job.payload.into_iter().collect());
    let inserted = sqlx::query(
        "INSERT INTO jobs \
         (id, producer_instance, queue, kind, payload, idempotency_key, status, max_attempts, available_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $8) \
         ON CONFLICT (producer_instance, idempotency_key) DO NOTHING",
    )
    .bind(&job.id)
    .bind(&job.producer)
    .bind(&job.queue)
    .bind(&job.kind)
    .bind(&payload)
    .bind(&job.idempotency_key)
    .bind(i32::try_from(job.max_attempts).expect("validated max attempts"))
    .bind(job.available_at)
    .execute(&mut *transaction)
    .await
    .map_err(db("insert job"))?;
    if inserted.rows_affected() == 1 {
        transaction.commit().await.map_err(db("commit enqueue"))?;
        return Ok(EnqueueOutcome::Created(job.id));
    }

    let existing = sqlx::query(
        "SELECT id, queue, kind, payload, available_at, max_attempts \
         FROM jobs WHERE producer_instance = $1 AND idempotency_key = $2 FOR UPDATE",
    )
    .bind(&job.producer)
    .bind(&job.idempotency_key)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(db("read idempotent job"))?;

    if let Some(row) = existing {
        let existing_payload: Value = row.try_get("payload").map_err(db("decode payload"))?;
        let existing_available: OffsetDateTime = row
            .try_get("available_at")
            .map_err(db("decode availability"))?;
        let same = row
            .try_get::<String, _>("queue")
            .map_err(db("decode queue"))?
            == job.queue
            && row
                .try_get::<String, _>("kind")
                .map_err(db("decode kind"))?
                == job.kind
            && existing_payload == payload
            && existing_available == job.available_at
            && row
                .try_get::<i32, _>("max_attempts")
                .map_err(db("decode max attempts"))?
                == i32::try_from(job.max_attempts).expect("validated max attempts");
        let id = row.try_get("id").map_err(db("decode job id"))?;
        transaction
            .commit()
            .await
            .map_err(db("commit idempotent enqueue"))?;
        return Ok(if same {
            EnqueueOutcome::Existing(id)
        } else {
            EnqueueOutcome::Conflict
        });
    }
    Err(JobsError::IdempotencyInvariant)
}

pub(crate) async fn claim(
    postgres: &OwnedPostgres,
    queue: &str,
    worker: &str,
    lease_token_hash: &[u8],
    lease_seconds: i64,
) -> Result<Option<ClaimResponse>, JobsError> {
    let mut transaction = postgres.pool().begin().await.map_err(db("begin claim"))?;
    sqlx::query(
        "WITH expired_exhausted AS ( \
           SELECT id FROM jobs \
           WHERE queue = $1 AND status = 'running' \
             AND lease_expires_at <= transaction_timestamp() \
             AND attempts >= max_attempts \
           ORDER BY lease_expires_at, id \
           FOR UPDATE SKIP LOCKED LIMIT $2 \
         ) \
         UPDATE jobs AS job SET status = 'failed', last_failure_code = 'attempts_exhausted', \
           lease_owner = NULL, lease_token_hash = NULL, lease_expires_at = NULL, \
           updated_at = transaction_timestamp() \
         FROM expired_exhausted WHERE job.id = expired_exhausted.id",
    )
    .bind(queue)
    .bind(EXPIRED_RETIREMENT_BATCH_LIMIT)
    .execute(&mut *transaction)
    .await
    .map_err(db("retire exhausted jobs"))?;

    let row = sqlx::query(
        "WITH candidate AS ( \
           SELECT id FROM jobs \
           WHERE queue = $1 AND attempts < max_attempts \
             AND ((status = 'queued' AND available_at <= transaction_timestamp()) \
               OR (status = 'running' AND lease_expires_at <= transaction_timestamp())) \
           ORDER BY available_at, created_at, id \
           FOR UPDATE SKIP LOCKED LIMIT 1 \
         ) \
         UPDATE jobs AS job SET status = 'running', attempts = job.attempts + 1, \
           lease_generation = job.lease_generation + 1, lease_owner = $2, lease_token_hash = $3, \
           lease_expires_at = transaction_timestamp() + ($4 * interval '1 second'), \
           updated_at = transaction_timestamp() \
         FROM candidate WHERE job.id = candidate.id \
         RETURNING job.id, job.queue, job.kind, job.payload, job.attempts, job.max_attempts, job.lease_expires_at",
    )
    .bind(queue)
    .bind(worker)
    .bind(lease_token_hash)
    .bind(lease_seconds)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(db("claim due job"))?;
    transaction.commit().await.map_err(db("commit claim"))?;
    row.as_ref().map(decode_claim).transpose()
}

pub(crate) async fn renew(
    postgres: &OwnedPostgres,
    job_id: &str,
    worker: &str,
    lease_token_hash: &[u8],
    lease_seconds: i64,
) -> Result<Option<OffsetDateTime>, JobsError> {
    sqlx::query_scalar(
        "UPDATE jobs SET lease_expires_at = transaction_timestamp() + ($4 * interval '1 second'), \
         updated_at = transaction_timestamp() \
         WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND lease_token_hash = $3 \
           AND lease_expires_at > transaction_timestamp() \
         RETURNING lease_expires_at",
    )
    .bind(job_id)
    .bind(worker)
    .bind(lease_token_hash)
    .bind(lease_seconds)
    .fetch_optional(postgres.pool())
    .await
    .map_err(db("renew job lease"))
}

pub(crate) async fn complete(
    postgres: &OwnedPostgres,
    job_id: &str,
    worker: &str,
    lease_token_hash: &[u8],
) -> Result<bool, JobsError> {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'succeeded', lease_owner = NULL, lease_token_hash = NULL, \
         lease_expires_at = NULL, updated_at = transaction_timestamp() \
         WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND lease_token_hash = $3 \
           AND lease_expires_at > transaction_timestamp()",
    )
    .bind(job_id)
    .bind(worker)
    .bind(lease_token_hash)
    .execute(postgres.pool())
    .await
    .map_err(db("complete job"))?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn fail(
    postgres: &OwnedPostgres,
    job_id: &str,
    worker: &str,
    lease_token_hash: &[u8],
    failure_code: &str,
    retryable: bool,
    retry_policy: RetryPolicy,
) -> Result<Option<FailResponse>, JobsError> {
    let mut transaction = postgres.pool().begin().await.map_err(db("begin fail"))?;
    let row = valid_lease(&mut transaction, job_id, worker, lease_token_hash).await?;
    let Some((attempts, max_attempts)) = row else {
        return Ok(None);
    };
    let retry = retryable && attempts < max_attempts;
    let delay = retry_delay(
        retry_policy.base_seconds,
        retry_policy.max_seconds,
        attempts,
    );
    let row = if retry {
        sqlx::query(
            "UPDATE jobs SET status = 'queued', available_at = transaction_timestamp() + ($2 * interval '1 second'), \
             last_failure_code = $3, lease_owner = NULL, lease_token_hash = NULL, lease_expires_at = NULL, \
             updated_at = transaction_timestamp() WHERE id = $1 RETURNING available_at",
        )
        .bind(job_id)
        .bind(delay)
        .bind(failure_code)
        .fetch_one(&mut *transaction)
        .await
        .map_err(db("requeue failed job"))?
    } else {
        sqlx::query(
            "UPDATE jobs SET status = 'failed', last_failure_code = $2, lease_owner = NULL, \
             lease_token_hash = NULL, lease_expires_at = NULL, updated_at = transaction_timestamp() \
             WHERE id = $1 RETURNING available_at",
        )
        .bind(job_id)
        .bind(failure_code)
        .fetch_one(&mut *transaction)
        .await
        .map_err(db("terminally fail job"))?
    };
    let available_at: OffsetDateTime = row
        .try_get("available_at")
        .map_err(db("decode retry availability"))?;
    transaction.commit().await.map_err(db("commit fail"))?;
    Ok(Some(FailResponse {
        status: if retry {
            FailResponseStatus::Queued
        } else {
            FailResponseStatus::Failed
        },
        available_at: retry.then(|| format_time(available_at)).transpose()?,
    }))
}

pub(crate) async fn inspect(
    postgres: &OwnedPostgres,
    job_id: &str,
) -> Result<Option<InspectResponse>, JobsError> {
    let row = sqlx::query(
        "SELECT id, queue, kind, status, attempts, max_attempts, available_at, lease_expires_at, \
         last_failure_code, created_at, updated_at FROM jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_optional(postgres.pool())
    .await
    .map_err(db("inspect job"))?;
    row.as_ref().map(decode_inspection).transpose()
}

async fn valid_lease(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: &str,
    worker: &str,
    lease_token_hash: &[u8],
) -> Result<Option<(i32, i32)>, JobsError> {
    let row = sqlx::query(
        "SELECT attempts, max_attempts FROM jobs \
         WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND lease_token_hash = $3 \
           AND lease_expires_at > transaction_timestamp() FOR UPDATE",
    )
    .bind(job_id)
    .bind(worker)
    .bind(lease_token_hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(db("validate job lease"))?;
    row.map(|row| {
        Ok((
            row.try_get("attempts").map_err(db("decode attempts"))?,
            row.try_get("max_attempts")
                .map_err(db("decode max attempts"))?,
        ))
    })
    .transpose()
}

fn decode_claim(row: &sqlx::postgres::PgRow) -> Result<ClaimResponse, JobsError> {
    let payload: Value = row
        .try_get("payload")
        .map_err(db("decode claimed payload"))?;
    let Value::Object(payload) = payload else {
        return Err(JobsError::InvalidStoredPayload);
    };
    let lease_expires_at: OffsetDateTime = row
        .try_get("lease_expires_at")
        .map_err(db("decode lease expiry"))?;
    Ok(ClaimResponse {
        job_id: row.try_get("id").map_err(db("decode job id"))?,
        queue: row.try_get("queue").map_err(db("decode queue"))?,
        kind: row.try_get("kind").map_err(db("decode kind"))?,
        payload: map_to_btree(payload),
        attempt: i64::from(
            row.try_get::<i32, _>("attempts")
                .map_err(db("decode attempts"))?,
        ),
        max_attempts: i64::from(
            row.try_get::<i32, _>("max_attempts")
                .map_err(db("decode max attempts"))?,
        ),
        lease_token: String::new(),
        lease_expires_at: format_time(lease_expires_at)?,
    })
}

fn decode_inspection(row: &sqlx::postgres::PgRow) -> Result<InspectResponse, JobsError> {
    let status: String = row.try_get("status").map_err(db("decode job status"))?;
    let status = match status.as_str() {
        "queued" => InspectResponseStatus::Queued,
        "running" => InspectResponseStatus::Running,
        "succeeded" => InspectResponseStatus::Succeeded,
        "failed" => InspectResponseStatus::Failed,
        _ => return Err(JobsError::InvalidStoredStatus),
    };
    let available_at: OffsetDateTime = row
        .try_get("available_at")
        .map_err(db("decode availability"))?;
    let lease_expires_at: Option<OffsetDateTime> = row
        .try_get("lease_expires_at")
        .map_err(db("decode lease expiry"))?;
    let created_at: OffsetDateTime = row.try_get("created_at").map_err(db("decode creation"))?;
    let updated_at: OffsetDateTime = row.try_get("updated_at").map_err(db("decode update"))?;
    Ok(InspectResponse {
        job_id: row.try_get("id").map_err(db("decode job id"))?,
        queue: row.try_get("queue").map_err(db("decode queue"))?,
        kind: row.try_get("kind").map_err(db("decode kind"))?,
        status,
        attempts: i64::from(
            row.try_get::<i32, _>("attempts")
                .map_err(db("decode attempts"))?,
        ),
        max_attempts: i64::from(
            row.try_get::<i32, _>("max_attempts")
                .map_err(db("decode max attempts"))?,
        ),
        available_at: format_time(available_at)?,
        lease_expires_at: Some(lease_expires_at.map(format_time).transpose()?),
        last_failure_code: Some(
            row.try_get("last_failure_code")
                .map_err(db("decode last failure"))?,
        ),
        created_at: format_time(created_at)?,
        updated_at: format_time(updated_at)?,
    })
}

fn map_to_btree(map: Map<String, Value>) -> BTreeMap<String, Value> {
    map.into_iter().collect()
}

fn retry_delay(base: i64, maximum: i64, attempts: i32) -> i64 {
    let exponent = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(62)
        .min(62);
    base.saturating_mul(1_i64 << exponent).min(maximum)
}

fn db(operation: &'static str) -> impl FnOnce(sqlx::Error) -> JobsError {
    move |source| JobsError::Database { operation, source }
}

#[cfg(test)]
mod tests {
    use super::retry_delay;

    #[test]
    fn retry_delay_is_exponential_and_bounded() {
        assert_eq!(retry_delay(5, 60, 1), 5);
        assert_eq!(retry_delay(5, 60, 2), 10);
        assert_eq!(retry_delay(5, 60, 3), 20);
        assert_eq!(retry_delay(5, 60, 20), 60);
    }
}
