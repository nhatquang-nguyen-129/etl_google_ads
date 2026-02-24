# Data Build Tool for Google Ads

## Purpose

- Use **dbt** to build Google Ads analytics-ready **materialized tables** in **Google BigQuery**

- Used **dbt** only for **SQL transformations** and all ELT processes are handled upstream

- Join Google Ads campaign insights fact tables with campaign metadata dim table

- Define final analytical grain and manage model dependencies using `ref()`

---

## Install

### Activate Python venv

- Create Python virtual environment with Python 3.13 interpreter if `venv\` folder not exists
```bash
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
```

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

---

### Install dbt adapter for Google BigQuery

- Install dbt adapter for Google BigQuery using the terminal
```bash
pip install dbt-core dbt-bigquery
```

- Verify installation and check installed dbt version
```bash
dbt --version
```

---

## Structure

### Models folder

- `models` is root folder for all dbt models and all logical separation by transformation stage

- `models/stg` is the staging layer providing a clean abstraction over ETL output tables and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['stg', 'campaign']
) }}
```

- `models/int` is the intermediate layer with the responsibilty to combine staging models then join with dimensions and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['stg', 'campaign']
) }}
```

- `models/mart` is the final materialization layer and materialized as `table` with example:
```bash
{{ config(
    materialized='table',
    tags=['stg', 'campaign']
) }}
```

---

### Config file

- `dbt_project.yml` is a required file for all dbt project which contains project operation instructions

- `profiles.yml` is a required file which contains the connection details for the data warehouse

---

## Deployment

### Manual Deployment

- Complie only with no execution
```bash
dbt compile
```

- Run all models
```bash
dbt build
```

- Run only budget reconciliation
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:mart
```

### Deployment with DAGs

- Using Python `subprocess` to call dbt for each stream
```bash
dbt_budget_reconcilie(
    google_cloud_project=PROJECT,
    select="tag:mart",
)
```