//! Durable single-step Jobs Module for Lenso applications.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration as StdDuration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use lenso_capability_jobs::{
    ClaimError, ClaimRequest, CompleteError, CompleteRequest, CompleteResponse, EnqueueError,
    EnqueueRequest, EnqueueResponse, FailError, FailRequest, InspectError, InspectRequest,
    JobsClaim, JobsComplete, JobsEndpoint, JobsEnqueue, JobsFail, JobsInspect, JobsProvider,
    JobsRenew, RenewError, RenewRequest, RenewResponse,
};
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_kernel::{
    DeactivateContext, InvocationContext, ModuleFuture, ModuleLifecycle, NativeRequestEndpoint,
    NativeRequestFuture, PrepareContext, RequestCapability, RuntimeFailure,
};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use zeroize::Zeroizing;

pub use operator::{JobsOperator, JobsOperatorError};

/// Package identity for the linked Rust Jobs Module.
pub const PACKAGE_ID: &str = "lenso.jobs";
/// Exact Cargo package version linked into the host.
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

const DEPENDENCY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

/// Immutable policy and resource references for one Jobs Module Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobsConfig {
    schema: String,
    database_url_secret: String,
    lease_seconds: i64,
    retry_base_seconds: i64,
    retry_max_seconds: i64,
    queues: Vec<String>,
    producer_instances: Vec<String>,
    worker_instances: Vec<String>,
    #[serde(default)]
    observer_instances: Vec<String>,
}

impl JobsConfig {
    /// Creates validated policy for one Jobs Module Instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        lease_seconds: i64,
        retry_base_seconds: i64,
        retry_max_seconds: i64,
        queues: Vec<String>,
        producer_instances: Vec<String>,
        worker_instances: Vec<String>,
    ) -> Result<Self, JobsConfigError> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            lease_seconds,
            retry_base_seconds,
            retry_max_seconds,
            queues,
            producer_instances,
            worker_instances,
            observer_instances: Vec::new(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Adds Module Instances allowed to inspect durable job state.
    pub fn with_observer_instances(
        mut self,
        observers: Vec<String>,
    ) -> Result<Self, JobsConfigError> {
        self.observer_instances = observers;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), JobsConfigError> {
        schema::schema_plan(self.schema.clone()).map_err(|_| JobsConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(JobsConfigError::InvalidSecretReference);
        }
        if !(1..=3_600).contains(&self.lease_seconds) {
            return Err(JobsConfigError::InvalidLeaseDuration);
        }
        if self.retry_base_seconds < 1
            || self.retry_max_seconds < self.retry_base_seconds
            || self.retry_max_seconds > 86_400
        {
            return Err(JobsConfigError::InvalidRetryPolicy);
        }
        validate_queues(&self.queues)?;
        validate_callers(&self.producer_instances, true)?;
        validate_callers(&self.worker_instances, true)?;
        validate_callers(&self.observer_instances, false)?;
        Ok(())
    }
}

/// Invalid immutable Jobs configuration supplied by App Composition.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum JobsConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("lease duration must be between 1 and 3600 seconds")]
    InvalidLeaseDuration,
    #[error("retry delays must be ordered and between 1 and 86400 seconds")]
    InvalidRetryPolicy,
    #[error("at least one valid queue is required")]
    InvalidQueues,
    #[error("configured queues must not contain duplicates")]
    DuplicateQueue,
    #[error("at least one authorized caller is required")]
    EmptyCallers,
    #[error("invalid authorized Module Instance")]
    InvalidCaller,
    #[error("authorized Module Instances must not contain duplicates")]
    DuplicateCaller,
}

/// Native Rust factory for the durable Jobs Provider.
#[derive(Clone, Copy, Debug, Default)]
pub struct JobsFactory;

impl NativeModuleFactory for JobsFactory {
    fn package_id(&self) -> &'static str {
        PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        if context.entrypoint() != "default" {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: format!("unsupported Jobs entrypoint `{}`", context.entrypoint()),
            });
        }
        let config: JobsConfig =
            serde_json::from_str(context.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("Jobs configuration is invalid: {error}"),
                }
            })?;
        config
            .validate()
            .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
                detail: format!("Jobs configuration is invalid: {error}"),
            })?;
        let state = Rc::new(RefCell::new(None));
        let provider = PostgresJobsProvider {
            config: Rc::new(config.clone()),
            state: state.clone(),
        };
        let endpoint = Rc::new(JobsEndpoint::new(provider)) as Rc<dyn NativeRequestEndpoint>;
        Ok(NativeModuleInstance::with_lifecycle(
            vec![endpoint],
            JobsLifecycle { config, state },
        ))
    }
}

#[derive(Clone)]
struct PostgresJobsProvider {
    config: Rc<JobsConfig>,
    state: Rc<RefCell<Option<OwnedPostgres>>>,
}

impl fmt::Debug for PostgresJobsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresJobsProvider")
            .field("schema", &self.config.schema)
            .field("prepared", &self.state.borrow().is_some())
            .finish_non_exhaustive()
    }
}

impl PostgresJobsProvider {
    fn prepared(&self) -> Result<OwnedPostgres, RuntimeFailure> {
        self.state
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::ModuleFailure {
                detail: "Jobs Module is not prepared".to_owned(),
            })
    }

    fn caller(context: &InvocationContext) -> Option<String> {
        context.caller_instance().map(ToOwned::to_owned)
    }

    fn producer(&self, context: &InvocationContext) -> Option<String> {
        Self::caller(context).filter(|caller| {
            self.config
                .producer_instances
                .iter()
                .any(|item| item == caller)
        })
    }

    fn queue_allowed(&self, queue: &str) -> bool {
        self.config.queues.iter().any(|allowed| allowed == queue)
    }

    fn worker(&self, context: &InvocationContext) -> Option<String> {
        Self::caller(context).filter(|caller| {
            self.config
                .worker_instances
                .iter()
                .any(|item| item == caller)
        })
    }

    fn can_inspect(&self, context: &InvocationContext) -> bool {
        Self::caller(context).is_some_and(|caller| {
            self.config
                .producer_instances
                .iter()
                .any(|item| item == &caller)
                || self
                    .config
                    .worker_instances
                    .iter()
                    .any(|item| item == &caller)
                || self
                    .config
                    .observer_instances
                    .iter()
                    .any(|item| item == &caller)
        })
    }
}

impl JobsProvider for PostgresJobsProvider {
    fn enqueue(
        &self,
        context: InvocationContext,
        request: EnqueueRequest,
    ) -> NativeRequestFuture<JobsEnqueue> {
        let Some(producer) = self.producer(&context) else {
            return domain::<JobsEnqueue>(EnqueueError::Unauthorized);
        };
        if !self.queue_allowed(&request.queue) || !valid_job_request(&request) {
            return domain::<JobsEnqueue>(EnqueueError::InvalidJob);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsEnqueue>(error),
        };
        Box::pin(async move {
            let available_at = parse_time(&request.available_at).map_err(runtime)?;
            let id = random_value("job_", 18).map_err(runtime)?;
            match storage::enqueue(
                &postgres,
                storage::NewJob {
                    id,
                    producer,
                    queue: request.queue,
                    kind: request.kind,
                    payload: request.payload,
                    idempotency_key: request.idempotency_key,
                    available_at,
                    max_attempts: request.max_attempts,
                },
            )
            .await
            .map_err(runtime)?
            {
                storage::EnqueueOutcome::Created(job_id) => Ok(Ok(EnqueueResponse {
                    job_id,
                    created: true,
                })),
                storage::EnqueueOutcome::Existing(job_id) => Ok(Ok(EnqueueResponse {
                    job_id,
                    created: false,
                })),
                storage::EnqueueOutcome::Conflict => Ok(Err(EnqueueError::IdempotencyConflict)),
            }
        })
    }

    fn claim(
        &self,
        context: InvocationContext,
        request: ClaimRequest,
    ) -> NativeRequestFuture<JobsClaim> {
        let Some(worker) = self.worker(&context) else {
            return domain::<JobsClaim>(ClaimError::Unauthorized);
        };
        if !valid_name(&request.queue, 128) || !self.queue_allowed(&request.queue) {
            return domain::<JobsClaim>(ClaimError::InvalidQueue);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsClaim>(error),
        };
        let lease_seconds = self.config.lease_seconds;
        Box::pin(async move {
            let token = random_value("job_lease_", 32).map_err(runtime)?;
            let hash = lease_hash(&token);
            let Some(mut claimed) =
                storage::claim(&postgres, &request.queue, &worker, &hash, lease_seconds)
                    .await
                    .map_err(runtime)?
            else {
                return Ok(Err(ClaimError::NoJobAvailable));
            };
            claimed.lease_token = token;
            Ok(Ok(claimed))
        })
    }

    fn renew(
        &self,
        context: InvocationContext,
        request: RenewRequest,
    ) -> NativeRequestFuture<JobsRenew> {
        let Some(worker) = self.worker(&context) else {
            return domain::<JobsRenew>(RenewError::Unauthorized);
        };
        if !valid_job_id(&request.job_id) || !valid_lease_token(&request.lease_token) {
            return domain::<JobsRenew>(RenewError::InvalidLease);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsRenew>(error),
        };
        let lease_seconds = self.config.lease_seconds;
        Box::pin(async move {
            let expiry = storage::renew(
                &postgres,
                &request.job_id,
                &worker,
                &lease_hash(&request.lease_token),
                lease_seconds,
            )
            .await
            .map_err(runtime)?;
            let Some(expiry) = expiry else {
                return Ok(Err(RenewError::InvalidLease));
            };
            Ok(Ok(RenewResponse {
                lease_expires_at: format_time(expiry).map_err(runtime)?,
            }))
        })
    }

    fn complete(
        &self,
        context: InvocationContext,
        request: CompleteRequest,
    ) -> NativeRequestFuture<JobsComplete> {
        let Some(worker) = self.worker(&context) else {
            return domain::<JobsComplete>(CompleteError::Unauthorized);
        };
        if !valid_job_id(&request.job_id) || !valid_lease_token(&request.lease_token) {
            return domain::<JobsComplete>(CompleteError::InvalidLease);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsComplete>(error),
        };
        Box::pin(async move {
            let completed = storage::complete(
                &postgres,
                &request.job_id,
                &worker,
                &lease_hash(&request.lease_token),
            )
            .await
            .map_err(runtime)?;
            if !completed {
                return Ok(Err(CompleteError::InvalidLease));
            }
            Ok(Ok(CompleteResponse { completed }))
        })
    }

    fn fail(
        &self,
        context: InvocationContext,
        request: FailRequest,
    ) -> NativeRequestFuture<JobsFail> {
        let Some(worker) = self.worker(&context) else {
            return domain::<JobsFail>(FailError::Unauthorized);
        };
        if !valid_job_id(&request.job_id) || !valid_lease_token(&request.lease_token) {
            return domain::<JobsFail>(FailError::InvalidLease);
        }
        if !valid_name(&request.failure_code, 128) {
            return domain::<JobsFail>(FailError::InvalidFailure);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsFail>(error),
        };
        let retry_base_seconds = self.config.retry_base_seconds;
        let retry_max_seconds = self.config.retry_max_seconds;
        Box::pin(async move {
            storage::fail(
                &postgres,
                &request.job_id,
                &worker,
                &lease_hash(&request.lease_token),
                &request.failure_code,
                request.retryable,
                storage::RetryPolicy {
                    base_seconds: retry_base_seconds,
                    max_seconds: retry_max_seconds,
                },
            )
            .await
            .map_err(runtime)?
            .map_or_else(
                || Ok(Err(FailError::InvalidLease)),
                |response| Ok(Ok(response)),
            )
        })
    }

    fn inspect(
        &self,
        context: InvocationContext,
        request: InspectRequest,
    ) -> NativeRequestFuture<JobsInspect> {
        if !self.can_inspect(&context) {
            return domain::<JobsInspect>(InspectError::Unauthorized);
        }
        if !valid_job_id(&request.job_id) {
            return domain::<JobsInspect>(InspectError::JobNotFound);
        }
        let postgres = match self.prepared() {
            Ok(postgres) => postgres,
            Err(error) => return failure::<JobsInspect>(error),
        };
        Box::pin(async move {
            storage::inspect(&postgres, &request.job_id)
                .await
                .map_err(runtime)?
                .map_or_else(
                    || Ok(Err(InspectError::JobNotFound)),
                    |response| Ok(Ok(response)),
                )
        })
    }
}

#[derive(Debug)]
struct JobsLifecycle {
    config: JobsConfig,
    state: Rc<RefCell<Option<OwnedPostgres>>>,
}

impl ModuleLifecycle for JobsLifecycle {
    fn prepare(&self, context: PrepareContext) -> ModuleFuture {
        let config = self.config.clone();
        let state = self.state.clone();
        let dependencies = context.dependencies().clone();
        let cancellation = context.cancellation();
        Box::pin(async move {
            let secrets = SecretsClient::from_dependencies(&dependencies)?;
            let invocation =
                dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
            let database_url = secrets
                .resolve_with_context(
                    invocation,
                    ResolveRequest {
                        reference: config.database_url_secret.clone(),
                    },
                )
                .await
                .map_err(|error| match error {
                    SecretsInvocationError::Domain(_) => RuntimeFailure::ModuleFailure {
                        detail: format!(
                            "Jobs database secret `{}` was rejected",
                            config.database_url_secret
                        ),
                    },
                    SecretsInvocationError::Runtime(error) => error,
                })?;
            let database_url = Zeroizing::new(database_url.value);
            let postgres = OwnedPostgres::prepare(
                &database_url,
                schema::schema_plan(config.schema).map_err(|error| {
                    RuntimeFailure::InvalidResolvedPlan {
                        detail: error.to_string(),
                    }
                })?,
            )
            .await
            .map_err(|error| RuntimeFailure::ModuleFailure {
                detail: error.to_string(),
            })?;
            state.replace(Some(postgres));
            Ok(())
        })
    }

    fn deactivate(&self, _context: DeactivateContext) -> ModuleFuture {
        let postgres = self.state.borrow_mut().take();
        Box::pin(async move {
            if let Some(postgres) = postgres {
                postgres.pool().close().await;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum JobsError {
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("stored job payload is not an object")]
    InvalidStoredPayload,
    #[error("stored job status is invalid")]
    InvalidStoredStatus,
    #[error("idempotency conflict did not resolve to a durable job")]
    IdempotencyInvariant,
    #[error("operating-system random source is unavailable")]
    Random,
    #[error("job timestamp is invalid")]
    Timestamp,
}

fn domain<C>(error: C::DomainError) -> NativeRequestFuture<C>
where
    C: RequestCapability,
{
    Box::pin(futures::future::ready(Ok(Err(error))))
}

fn failure<C>(error: RuntimeFailure) -> NativeRequestFuture<C>
where
    C: RequestCapability,
{
    Box::pin(futures::future::ready(Err(error)))
}

fn validate_callers(values: &[String], required: bool) -> Result<(), JobsConfigError> {
    if required && values.is_empty() {
        return Err(JobsConfigError::EmptyCallers);
    }
    if values.iter().any(|value| !valid_name(value, 256)) {
        return Err(JobsConfigError::InvalidCaller);
    }
    let unique = values.iter().collect::<BTreeSet<_>>();
    if unique.len() != values.len() {
        return Err(JobsConfigError::DuplicateCaller);
    }
    Ok(())
}

fn validate_queues(values: &[String]) -> Result<(), JobsConfigError> {
    if values.is_empty() || values.iter().any(|value| !valid_name(value, 128)) {
        return Err(JobsConfigError::InvalidQueues);
    }
    if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(JobsConfigError::DuplicateQueue);
    }
    Ok(())
}

fn valid_job_request(request: &EnqueueRequest) -> bool {
    valid_name(&request.queue, 128)
        && valid_name(&request.kind, 256)
        && valid_name(&request.idempotency_key, 256)
        && (1..=100).contains(&request.max_attempts)
        && serde_json::to_vec(&request.payload)
            .is_ok_and(|payload| payload.len() <= MAX_PAYLOAD_BYTES)
        && parse_time(&request.available_at).is_ok()
}

fn valid_name(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn valid_job_id(value: &str) -> bool {
    valid_random_value(value, "job_", 24)
}

fn valid_lease_token(value: &str) -> bool {
    valid_random_value(value, "job_lease_", 43)
}

fn valid_random_value(value: &str, prefix: &str, encoded_length: usize) -> bool {
    value.strip_prefix(prefix).is_some_and(|encoded| {
        encoded.len() == encoded_length
            && encoded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    })
}

fn random_value(prefix: &str, length: usize) -> Result<String, JobsError> {
    let mut bytes = vec![0_u8; length];
    getrandom::fill(&mut bytes).map_err(|_| JobsError::Random)?;
    Ok(format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn lease_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

fn parse_time(value: &str) -> Result<OffsetDateTime, JobsError> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| JobsError::Timestamp)?;
    timestamp
        .replace_nanosecond((timestamp.nanosecond() / 1_000) * 1_000)
        .map_err(|_| JobsError::Timestamp)
}

pub(crate) fn format_time(value: OffsetDateTime) -> Result<String, JobsError> {
    value.format(&Rfc3339).map_err(|_| JobsError::Timestamp)
}

fn runtime(error: impl fmt::Display) -> RuntimeFailure {
    RuntimeFailure::ModuleFailure {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, rc::Rc};

    use futures::future::LocalBoxFuture;
    use lenso_app_plan::{
        AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
        ModuleInstancePlan, ResolvedAppPlan,
    };
    use lenso_capability_jobs::{
        CAPABILITY_ID, CLAIM_OPERATION, COMPLETE_OPERATION, ENQUEUE_OPERATION, FAIL_OPERATION,
        INSPECT_OPERATION, RENEW_OPERATION,
    };
    use lenso_capability_secrets::{
        CAPABILITY_ID as SECRETS_CAPABILITY_ID, DESCRIPTOR_VERSION as SECRETS_DESCRIPTOR_VERSION,
        RESOLVE_OPERATION, ResolveError, ResolveRequest, ResolveResponse, SecretsEndpoint,
        SecretsProvider,
    };
    use lenso_kernel::{DeterministicDriver, Kernel, NativeRequestEndpoint};
    use lenso_native_adapter::{NativeModuleInstance, NativeModuleRegistry};

    use super::*;

    const PRODUCER_PACKAGE: &str = "test.jobs-producer";
    const WORKER_PACKAGE: &str = "test.jobs-worker";
    const SECRETS_PACKAGE: &str = "test.jobs-secrets";

    #[derive(Debug)]
    struct EmptyFactory(&'static str);

    impl NativeModuleFactory for EmptyFactory {
        fn package_id(&self) -> &'static str {
            self.0
        }

        fn instantiate(
            &self,
            _context: NativeModuleFactoryContext<'_>,
        ) -> Result<NativeModuleInstance, RuntimeFailure> {
            Ok(NativeModuleInstance::default())
        }
    }

    #[derive(Clone, Debug)]
    struct FakeSecrets;

    impl SecretsProvider for FakeSecrets {
        fn resolve(
            &self,
            _context: InvocationContext,
            _request: ResolveRequest,
        ) -> LocalBoxFuture<'static, Result<Result<ResolveResponse, ResolveError>, RuntimeFailure>>
        {
            Box::pin(futures::future::ready(Ok(Ok(ResolveResponse {
                value: "postgres://unused".to_owned(),
            }))))
        }
    }

    #[derive(Debug)]
    struct FakeSecretsFactory;

    impl NativeModuleFactory for FakeSecretsFactory {
        fn package_id(&self) -> &'static str {
            SECRETS_PACKAGE
        }

        fn instantiate(
            &self,
            _context: NativeModuleFactoryContext<'_>,
        ) -> Result<NativeModuleInstance, RuntimeFailure> {
            let endpoint =
                Rc::new(SecretsEndpoint::new(FakeSecrets)) as Rc<dyn NativeRequestEndpoint>;
            Ok(NativeModuleInstance::new(vec![endpoint]))
        }
    }

    fn config() -> JobsConfig {
        JobsConfig::new(
            "jobs",
            "jobs/database",
            30,
            5,
            300,
            vec!["email".to_owned()],
            vec!["orders".to_owned()],
            vec!["worker".to_owned()],
        )
        .unwrap()
    }

    fn plan(configuration: String) -> ResolvedAppPlan {
        let producer = ModuleInstancePlan::new("producer", PRODUCER_PACKAGE)
            .with_requirement(CapabilityRequirementPlan::one(CAPABILITY_ID, "1.0.0"));
        let worker = ModuleInstancePlan::new("worker", WORKER_PACKAGE)
            .with_requirement(CapabilityRequirementPlan::one(CAPABILITY_ID, "1.0.0"));
        let jobs = ModuleInstancePlan::new("jobs", PACKAGE_ID)
            .with_configuration(configuration)
            .with_capability(CapabilityEndpointPlan::new(
                CAPABILITY_ID,
                "1.0.0",
                [
                    ENQUEUE_OPERATION,
                    CLAIM_OPERATION,
                    RENEW_OPERATION,
                    COMPLETE_OPERATION,
                    FAIL_OPERATION,
                    INSPECT_OPERATION,
                ],
            ))
            .with_requirement(CapabilityRequirementPlan::one(
                SECRETS_CAPABILITY_ID,
                SECRETS_DESCRIPTOR_VERSION,
            ));
        let secrets = ModuleInstancePlan::new("secrets", SECRETS_PACKAGE).with_capability(
            CapabilityEndpointPlan::new(
                SECRETS_CAPABILITY_ID,
                SECRETS_DESCRIPTOR_VERSION,
                [RESOLVE_OPERATION],
            ),
        );
        AppComposition::new(
            vec![producer, worker, jobs, secrets],
            vec![
                CapabilityBinding::new("producer", CAPABILITY_ID, "1.0.0", "jobs"),
                CapabilityBinding::new("worker", CAPABILITY_ID, "1.0.0", "jobs"),
                CapabilityBinding::new(
                    "jobs",
                    SECRETS_CAPABILITY_ID,
                    SECRETS_DESCRIPTOR_VERSION,
                    "secrets",
                ),
            ],
        )
        .resolve()
        .expect("Jobs test Composition should resolve")
    }

    #[test]
    fn configuration_rejects_unsafe_or_unbounded_policy() {
        let mut invalid = config();
        invalid.schema = "public".to_owned();
        assert_eq!(invalid.validate(), Err(JobsConfigError::InvalidSchema));

        let mut invalid = config();
        invalid.lease_seconds = 0;
        assert_eq!(
            invalid.validate(),
            Err(JobsConfigError::InvalidLeaseDuration)
        );

        let mut invalid = config();
        invalid.worker_instances = vec!["worker".to_owned(), "worker".to_owned()];
        assert_eq!(invalid.validate(), Err(JobsConfigError::DuplicateCaller));
    }

    #[test]
    fn identifiers_and_lease_tokens_are_strict() {
        let job_id = random_value("job_", 18).unwrap();
        let lease = random_value("job_lease_", 32).unwrap();
        assert!(valid_job_id(&job_id));
        assert!(valid_lease_token(&lease));
        assert!(!valid_lease_token("job_lease_not-a-token"));
        assert_ne!(lease_hash(&lease), lease.as_bytes());
    }

    #[test]
    fn enqueue_validation_bounds_portable_payloads() {
        let request = EnqueueRequest {
            available_at: "2026-08-24T12:00:00Z".to_owned(),
            idempotency_key: "order-42".to_owned(),
            kind: "orders.capture".to_owned(),
            max_attempts: 3,
            payload: [("order_id".to_owned(), serde_json::json!("42"))]
                .into_iter()
                .collect(),
            queue: "orders".to_owned(),
        };
        assert!(valid_job_request(&request));
        assert!(!format!("{request:?}").contains("order_id"));
    }

    #[test]
    fn undeclared_queues_fail_before_storage_access() {
        let provider = PostgresJobsProvider {
            config: Rc::new(config()),
            state: Rc::new(RefCell::new(None)),
        };
        let mut enqueue = EnqueueRequest {
            available_at: "2026-08-24T12:00:00Z".to_owned(),
            idempotency_key: "order-42".to_owned(),
            kind: "orders.capture".to_owned(),
            max_attempts: 3,
            payload: BTreeMap::new(),
            queue: "billing".to_owned(),
        };
        let enqueue_result = futures::executor::block_on(
            provider.enqueue(
                InvocationContext::new(1, None, lenso_kernel::CancellationToken::new())
                    .with_caller_instance("orders"),
                enqueue.clone(),
            ),
        )
        .unwrap();
        assert_eq!(enqueue_result, Err(EnqueueError::InvalidJob));

        enqueue.queue = "email".to_owned();
        let storage_failure = futures::executor::block_on(
            provider.enqueue(
                InvocationContext::new(2, None, lenso_kernel::CancellationToken::new())
                    .with_caller_instance("orders"),
                enqueue,
            ),
        );
        assert!(matches!(
            storage_failure,
            Err(RuntimeFailure::ModuleFailure { detail }) if detail.contains("not prepared")
        ));
    }

    #[test]
    fn invalid_configuration_fails_before_module_preparation() {
        let configuration = serde_json::json!({
            "schema": "public",
            "database_url_secret": "jobs/database",
            "lease_seconds": 30,
            "retry_base_seconds": 5,
            "retry_max_seconds": 300,
            "queues": ["email"],
            "producer_instances": ["producer"],
            "worker_instances": ["worker"]
        })
        .to_string();
        let driver = DeterministicDriver::new();
        let result = driver.run(Kernel::start_native(
            plan(configuration),
            driver.clone(),
            NativeModuleRegistry::new()
                .with_factory(EmptyFactory(PRODUCER_PACKAGE))
                .with_factory(EmptyFactory(WORKER_PACKAGE))
                .with_factory(FakeSecretsFactory)
                .with_factory(JobsFactory),
        ));
        assert!(matches!(
            result,
            Err(RuntimeFailure::InvalidResolvedPlan { detail })
                if detail.contains("invalid owned PostgreSQL schema")
        ));
    }

    #[test]
    fn removing_jobs_leaves_no_kernel_or_composition_requirement() {
        let remaining = AppComposition::new(
            vec![
                ModuleInstancePlan::new("producer", PRODUCER_PACKAGE),
                ModuleInstancePlan::new("worker", WORKER_PACKAGE),
            ],
            vec![],
        )
        .resolve()
        .expect("App without Jobs behavior should still resolve");
        assert_eq!(remaining.module_instances().len(), 2);
        assert!(remaining.capability_bindings().is_empty());
    }
}
