# 📊 KidsPlaza Google Ads Pipeline

KidsPlaza Google Ads Pipeline powers the **Digital Advertising Data Analysis** process at KidsPlaza by automating the ingestion, standardization, and loading of Google Ads data into the centralized analytics warehouse (**Google BigQuery**).

This pipeline integrates **Google Ads Insights data**, supporting both **daily synchronization** and **historical backfills**, and serves as the foundation for performance analysis, budget reconciliation, and marketing reporting.

The system is designed with a **modular, stage-based architecture** to ensure maintainability, scalability, and controlled evolution over time.

---

## 📌 Current Branch

> **This README documents the behavior and scope of the current development branch:**  
> **`branch_1x`**

`branch_1x` represents the active **1.x.x development line**, where incremental features, framework enhancements, and non-breaking changes are implemented before being promoted to production (`main`).

---

## 🧱 Pipeline Stage Overview

The pipeline is organized into independent, reusable stages:

- **Fetch**
  - Retrieve Google Ads data via the Google Ads API
  - Supports daily sync and configurable date-range backfills
- **Schema**
  - Enforce predefined schemas (column presence, data types, naming conventions)
  - Acts as a contract layer between raw Ads data and downstream processing
- **Enrich**
  - Add derived metrics, normalized dimensions, and business-specific mappings
- **Ingest**
  - Load validated data into the raw storage layer in BigQuery
- **Staging**
  - Prepare cleaned, analysis-friendly intermediate tables
- **Mart**
  - Produce final datasets optimized for reporting and descriptive analytics

Each stage is implemented as an isolated component to minimize coupling and simplify debugging, testing, and future refactoring.

---

## 🎯 Use Cases

- Centralized analysis of Google Ads performance:
  - Spend, impressions, clicks, conversions
- Budget reconciliation:
  - Planned vs actual spend
  - Campaign- and program-level tracking
- Descriptive analytics:
  - Performance trends over time
  - Campaign, channel, and content effectiveness
- Data quality validation:
  - Schema consistency
  - Missing or anomalous metrics

---

## 🔧 Configuration

Pipeline behavior is controlled via environment variables, allowing the same codebase to operate across environments and execution contexts:

- `FETCH_BACKEND` — data source backend (e.g. `google_ads`)
- `FETCH_DIRECTION` — customer ID, account scope, or query configuration
- `DOMAIN` — logical domain identifier (e.g. `ads`)
- `START_DATE` / `END_DATE` — optional date range for historical backfills

This approach ensures a **clean separation between business logic and runtime configuration**.

---

## 🚀 Branching & Deployment Strategy

This repository follows a **versioned, production-first branching model**, inspired by large-scale OSS projects (e.g. Apache Lucene) and adapted for internal data pipelines.

### Branch Roles

| Branch | Purpose |
|------|--------|
| `main` | **Production** – stable, deployed pipeline |
| `current_branch` | Active development for oldest version **x.x** (current branch) |
| `development_branch` | Major architectural redesigns or framework rewrites |

---

### Deployment Flow

```text
current_branch → main → deploy

## 🏢 Ownership & Maintenance

This repository is maintained by the **Data & Analytics / Digital Team at KidsPlaza**.

For questions, access requests, or contributions, please contact:

- **Internal:** quang.nn@kidsplaza.vn  
- **External:** nhatquang.nguyen.129@gmail.com  
- Or reach out via the internal Slack channel **#data-engineering**

---

## ⚠️ Disclaimer

This project is intended for **internal use only**.

It contains custom business logic tailored specifically to KidsPlaza’s retail data structures, sales processes, reconciliation rules, and naming conventions.  
Do **not** reuse, replicate, or adapt this codebase outside of KidsPlaza without prior approval.

---

## 📄 License

All content and source code in this repository is **proprietary to KidsPlaza**.

Redistribution, publication, or open-sourcing of any part of this project is strictly prohibited without explicit written consent from the company.

---

## 🤖 AI-Assisted Development

This repository includes code, documentation, and architectural guidance that has been **partially developed or enhanced using AI tools** (e.g. GitHub Copilot, ChatGPT by OpenAI), under the supervision of the development team.

All AI-assisted output has been reviewed, validated, and adapted to meet KidsPlaza’s internal engineering and production standards.