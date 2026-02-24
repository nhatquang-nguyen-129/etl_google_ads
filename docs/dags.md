# DAGs for Google Ads

## Purpose

- Execute Budget Reconciliation DAGs using **predefined runtime configurations**

- Resolve **execution time window** indirectly via `main.py` instead of `--input_month` via argparse

- Designed primarily for **production deployment** on Cloud Run

- Automatically load **required secrets** from Google Secret Manager

- Dispatch execution to the **DAG orchestrator** without exposing manual entrypoints

## Execution

### Runtime Contract

 - The following environment variables `COMPANY`, `PROJECT`, `DEPARTMENT` and `ACCOUNT` must be provided

- The DAGS execution time logic is controlled externally by `main.py` with predefined runtime modes

- The resolved execution context is then passed to `dags_google_ads`

### Secret Management

- `main.py` initializes Google Secret Manager client to resolves required secrets

- `main.py` retrieves `secret_customer_id` from `{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}`

- `main.py` retrieves `secret_customer_name` from `projects/{PROJECT}/secrets/{SECRET_NAME}/versions/latest`

- `main.py` retrieves `secret_credentials_json` from `{COMPANY}_secret_all_google_token_access_user`

- `main.py` retrieves `secret_credentials_name ` from `projects/{PROJECT}/secrets/{secret_credentials_json}/versions/latest`

### Production Deployment

- This runtime is intended to run inside `Cloud Run` and triggered by `Cloud Scheduler`

- This runtime is technically possible for local execution but not recommended

- For manual historical reprocessing, use the `Backfill` module instead

- Run DAGs for specific `MODE` using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"
$env:MODE="your-time-window"

python main.py
```