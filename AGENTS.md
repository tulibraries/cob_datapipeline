# AGENT Instructions for cob_datapipeline

## Purpose
- Airflow DAGs and helper scripts for Temple Libraries indexing workflows (runs inside Airflow; see README for details).

## Development workflow
- Use `pipenv install --dev` for dependencies; run `pipenv run pylint cob_datapipeline` before PRs.
- Do not run test commands such as `pipenv run pytest` unless the user explicitly asks for tests.
- Never open a pull request unless the user explicitly asks for one.
- Makefile targets rely on the `airflow-docker-dev-setup` submodule; use those for local Airflow runs instead of ad-hoc commands.
- Keep edits ASCII and small; prefer `apply_patch`. Never revert user changes or secrets.

## Airflow/DAG notes
- BashOperator `bash_command` paths should end with a trailing space to prevent Airflow/Jinja from treating file paths as templates.
- DAG configuration (URLs, S3 buckets, Solr settings) comes from Airflow Variables/Connections; do not hardcode credentials.

## Data/scripts
- Scripts live in `cob_datapipeline/scripts`; keep them executable and POSIX-compliant. Avoid repo-specific absolute paths—use env vars from the DAGs.

## Testing/deploy
- PRs are linted/tested by GitHub Actions; pushes to `main` deploy to QA and tags `v*.*.*` deploy to prod.
