use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

use lenso_capability_jobs::{
    ClaimError, ClaimRequest, CompleteError, CompleteRequest, EnqueueError, EnqueueRequest,
    FailRequest, FailResponseStatus, InspectRequest, InspectResponseStatus, JobsProvider,
};
use lenso_kernel::{CancellationToken, InvocationContext};
use lenso_postgres_kit::OwnedPostgres;
use sqlx::{AssertSqlSafe, Connection};
use time::OffsetDateTime;
use url::Url;

use super::{JobsConfig, JobsOperator, PostgresJobsProvider, format_time, lease_hash, schema};

fn context(caller: &str, request_id: u64) -> InvocationContext {
    InvocationContext::new(request_id, None, CancellationToken::new()).with_caller_instance(caller)
}

fn request(key: &str, kind: &str, max_attempts: i64) -> EnqueueRequest {
    EnqueueRequest {
        queue: "email".to_owned(),
        kind: kind.to_owned(),
        payload: BTreeMap::from([("message_id".to_owned(), serde_json::json!("msg-42"))]),
        idempotency_key: key.to_owned(),
        available_at: format_time(OffsetDateTime::now_utc()).unwrap(),
        max_attempts,
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn durable_jobs_preserve_idempotency_fencing_retry_and_terminal_state() {
    let Some(database_url) = std::env::var("LENSO_JOBS_TEST_DATABASE_URL").ok() else {
        eprintln!("skipping PostgreSQL acceptance; LENSO_JOBS_TEST_DATABASE_URL is unset");
        return;
    };
    let parsed = Url::parse(&database_url).expect("test database URL must be valid");
    let database = parsed.path().trim_start_matches('/');
    assert!(
        database.starts_with("lenso_jobs_test"),
        "acceptance requires a disposable lenso_jobs_test* database"
    );

    let schema_name = format!("jobs_acceptance_{}", std::process::id());
    let mut cleanup = sqlx::PgConnection::connect(&database_url).await.unwrap();
    let drop_schema = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();

    JobsOperator::setup(&database_url, &schema_name)
        .await
        .expect("operator setup should install the Jobs schema");
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.as_str()).unwrap(),
    )
    .await
    .expect("runtime preparation should verify the exact Jobs schema");
    let config = JobsConfig::new(
        &schema_name,
        "jobs/database",
        1,
        1,
        4,
        vec!["email".to_owned()],
        vec!["producer".to_owned()],
        vec!["worker".to_owned()],
    )
    .unwrap()
    .with_observer_instances(vec!["observer".to_owned()])
    .unwrap();
    let provider = PostgresJobsProvider {
        config: Rc::new(config),
        state: Rc::new(RefCell::new(Some(postgres.clone()))),
    };

    let enqueue = request("message-42", "email.send", 3);
    let first = provider
        .enqueue(context("producer", 1), enqueue.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(first.created);
    let duplicate = provider
        .enqueue(context("producer", 2), enqueue)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(duplicate.job_id, first.job_id);
    assert!(!duplicate.created);

    let conflict = provider
        .enqueue(
            context("producer", 3),
            request("message-42", "email.delete", 3),
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(conflict, EnqueueError::IdempotencyConflict);
    let unauthorized = provider
        .enqueue(context("intruder", 4), request("intruder", "email.send", 3))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(unauthorized, EnqueueError::Unauthorized);

    let claimed = provider
        .claim(
            context("worker", 5),
            ClaimRequest {
                queue: "email".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job_id, first.job_id);
    assert_eq!(claimed.attempt, 1);
    assert!(!format!("{claimed:?}").contains(&claimed.lease_token));
    assert!(!format!("{claimed:?}").contains("msg-42"));

    let running = provider
        .inspect(
            context("observer", 6),
            InspectRequest {
                job_id: first.job_id.clone(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.status, InspectResponseStatus::Running);
    assert_eq!(running.attempts, 1);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let reclaimed = provider
        .claim(
            context("worker", 7),
            ClaimRequest {
                queue: "email".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.job_id, first.job_id);
    assert_eq!(reclaimed.attempt, 2);
    assert_ne!(
        lease_hash(&reclaimed.lease_token),
        lease_hash(&claimed.lease_token)
    );

    let fenced = provider
        .complete(
            context("worker", 8),
            CompleteRequest {
                job_id: first.job_id.clone(),
                lease_token: claimed.lease_token,
            },
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(fenced, CompleteError::InvalidLease);
    provider
        .complete(
            context("worker", 9),
            CompleteRequest {
                job_id: first.job_id.clone(),
                lease_token: reclaimed.lease_token,
            },
        )
        .await
        .unwrap()
        .unwrap();
    let succeeded = provider
        .inspect(
            context("producer", 10),
            InspectRequest {
                job_id: first.job_id,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(succeeded.status, InspectResponseStatus::Succeeded);

    let retry_job = provider
        .enqueue(context("producer", 11), request("retry", "email.send", 2))
        .await
        .unwrap()
        .unwrap();
    let retry_claim = provider
        .claim(
            context("worker", 12),
            ClaimRequest {
                queue: "email".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let retried = provider
        .fail(
            context("worker", 13),
            FailRequest {
                job_id: retry_job.job_id.clone(),
                lease_token: retry_claim.lease_token,
                failure_code: "provider_unavailable".to_owned(),
                retryable: true,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.status, FailResponseStatus::Queued);
    let empty = provider
        .claim(
            context("worker", 14),
            ClaimRequest {
                queue: "email".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(empty, ClaimError::NoJobAvailable);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let final_claim = provider
        .claim(
            context("worker", 15),
            ClaimRequest {
                queue: "email".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let failed = provider
        .fail(
            context("worker", 16),
            FailRequest {
                job_id: retry_job.job_id.clone(),
                lease_token: final_claim.lease_token,
                failure_code: "invalid_recipient".to_owned(),
                retryable: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, FailResponseStatus::Failed);
    let terminal = provider
        .inspect(
            context("observer", 17),
            InspectRequest {
                job_id: retry_job.job_id,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, InspectResponseStatus::Failed);
    assert_eq!(
        terminal.last_failure_code,
        Some(Some("invalid_recipient".to_owned()))
    );

    postgres.pool().close().await;
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();
}
