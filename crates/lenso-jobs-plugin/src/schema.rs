use lenso_postgres_kit::{Migration, PlanError, SchemaPlan, sql_migrations};

const MIGRATIONS: &[Migration] = sql_migrations![
    (1, "create-jobs", "migrations/001_create_jobs.sql",),
    (
        2,
        "index-expired-job-maintenance",
        "migrations/002_index_expired_job_maintenance.sql",
    ),
];

pub(crate) fn schema_plan(schema: impl Into<std::sync::Arc<str>>) -> Result<SchemaPlan, PlanError> {
    SchemaPlan::new(schema, MIGRATIONS)
}
