# dbt — Project Configuration (dbt_project.yml)


---

## dbt_project.yml

- name: dbt project identifier used as namespace for model configuration and must match the top-level folder under models
- version: Used for tracking breaking changes in mart schema
- config-version: dbt config schema version required for scoped model configs
- profile: Defines BigQuery project/dataset/authentication method
- model-paths: ["models"] contains SQL models only
- macro-paths: ["macros"] contains SQL helper macros only

---

## stg
- Staging models: Not materialized
```bash
stg:
  +materialized: ephemeral
```
---

## int
- Intermediate models: Not materialized
```bash
int:
  +materialized: ephemeral
```

---

## mart
- Mart models: Persisted as physical tables in BigQuery
```bash
mart:
  +materialized: table
  +tags: ["mart"]

```
- Tagged for selective execution:
```bash
dbt run --select tag:mart
```