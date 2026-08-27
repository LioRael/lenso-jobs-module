use lenso_postgres_kit::{
    OwnedPostgres, PostgresKitError, SchemaOperator, SetupOutcome, UpgradeOutcome,
};
use thiserror::Error;

use crate::schema::schema_plan;

/// Explicit schema administration for one Jobs Plugin Instance.
#[derive(Clone, Debug)]
pub struct JobsOperator {
    postgres: OwnedPostgres,
}

impl JobsOperator {
    /// Creates a missing Jobs schema and applies the complete authored plan.
    pub async fn setup(
        database_url: &str,
        schema: &str,
    ) -> Result<SetupOutcome, JobsOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .setup()
            .await?)
    }

    /// Applies pending Jobs migrations to an existing managed schema.
    pub async fn upgrade(
        database_url: &str,
        schema: &str,
    ) -> Result<UpgradeOutcome, JobsOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .upgrade()
            .await?)
    }

    /// Connects only when the exact authored schema is already installed.
    pub async fn connect(database_url: &str, schema: &str) -> Result<Self, JobsOperatorError> {
        Ok(Self {
            postgres: OwnedPostgres::prepare(database_url, schema_plan(schema)?).await?,
        })
    }

    /// Returns the verified Module-owned schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        self.postgres.schema()
    }
}

/// Failure from an explicit Jobs schema operator workflow.
#[derive(Debug, Error)]
pub enum JobsOperatorError {
    #[error(transparent)]
    Plan(#[from] lenso_postgres_kit::PlanError),
    #[error(transparent)]
    Postgres(#[from] PostgresKitError),
}
