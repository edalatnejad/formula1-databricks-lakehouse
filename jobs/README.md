# Databricks job definitions

These files document the two jobs used in the Formula 1 project.

- `incremental_refresh.yml`: dependency-based Bronze, Silver and Gold processing.
- `batch_orchestration.yml`: batch discovery, conditional execution, refresh-job invocation and completion/failure handling.

Personal workspace paths have been replaced with repository-relative notebook paths. The refresh job ID is a placeholder. These are portfolio examples, not directly deployable job definitions. Configure notebook paths and the target job ID for your own Databricks workspace.
