# Google Ads API — Authentication & Credential Bootstrap

## Purpose

- Authenticate **Google Ads API** for internal analytics & reporting
- Generate a long-lived **refresh token** via OAuth
- Enable **non-interactive** access in production environments
- Avoid using Service Accounts (not supported by Google Ads API)

---

## What This Auth Flow Does

- Perform **one-time OAuth login** via browser
- Generate Google Ads credentials:
  - developer token
  - OAuth client ID & secret
  - refresh token
  - login customer ID (MCC)
- Output a reusable **credential payload**
- Designed for **read-only analytics workloads**

---

## What This Auth Flow Does NOT Do

- Run in production
- Store secrets automatically (by default)
- Rotate credentials
- Handle ETL or data ingestion
- Replace Google Ads permission management

---

## Prerequisites

- Google Cloud Project with **Google Ads API enabled**
- Approved **Developer Token**
- OAuth Client ID:
  - Type: **Desktop App**
  - Client secret JSON downloaded at creation time
- Google account with access to:
  - Google Ads account
  - MCC (if applicable)

---

## Why OAuth (Not Service Account)

- Google Ads API **does NOT support Service Accounts**
- All access must be **user-based OAuth**
- OAuth is safer and more flexible for:
  - analytics pipelines
  - reporting jobs
  - controlled read-only access

---

## Bootstrap Flow (Run Once)

### What happens

- Script opens browser for OAuth consent
- User logs in and approves access
- Script receives:
  - access token
  - refresh token
- Credentials are assembled into a single payload

### How to run

```bash
python bootstrap_google_ads_oauth.py
