# Google Ads Bootstrap for Authentication

## Purpose

- Generate a long-lived **refresh token** via **one-time OAuth login** via browser

- Generate a reusable Google Ads credentials payload which only needs to be run only **ONCE**

- Enable **non-interactive** access in production environments

- Avoid using Service Accounts which is not supported by Google Ads API

---


## Install

### Prerequisites for Bootstrap Authentication

- Google Ads **Developer Token** from Google Ads MCC UI

- Google Cloud Project with enabled **Google Ads API**

- Google Account with access to Google Ads MCC

- OAuth Client ID from a Google Cloud **Desktop App**

- Downloaded JSON secret client at OAuth2 App creation time 

---

### Run Bootstrap Authentication

- Fill required variables
```bash
GOOGLE_ADS_MCC_DEVELOPER_TOKEN = "PUT_YOUR_GOOGLE_ADS_MCC_DEVELOPER_TOKEN_HERE"
GOOGLE_ADS_MCC_CUSTOMER_ID = "PUT_YOUR_GOOGLE_ADS_MCC_CUSTOMER_ID_HERE"
```

- Run `google_ads_oauth.py` to get assembled single payload credentials
```bash
python google_ads_oauth.py
```

- Script opens browser for OAuth consent and example console output below:
`
Successfully generated credential payload:

{
  "developer_token": "AIzaSyD-EXAMPLE-DEVELOPER-TOKEN",
  "client_id": "1234567890-abcdefg.apps.googleusercontent.com",
  "client_secret": "GOCSPX-EXAMPLE-CLIENT-SECRET",
  "refresh_token": "1//0gEXAMPLE_REFRESH_TOKEN_LONG_STRING",
  "login_customer_id": "1234567890"
}
`

- Copy the credential payload above then Store it securely with Secret Manager

- Use the credential payload to initialize GoogleAdsClient

## Revoke



Token Lifecycle & Revocation Behavior
Important: Revoking Access Invalidates Refresh Token

If you revoke the app from:

Google Account → Security → Third-party apps


Google will:

Immediately invalidate the refresh token

Invalidate all access tokens derived from that refresh token

Return invalid_grant on the next token exchange attempt

This will break:

Google Ads API client initialization

Any non-interactive production workload

Scheduled pipelines (Airflow / Cron / etc.)

Other Events That Invalidate Refresh Tokens

Refresh tokens may also become invalid if:

OAuth client ID is changed or deleted

OAuth consent screen is modified significantly

Google account password is reset

Token is unused for ~6 months

Account security policy forces re-authentication

When this happens, API calls will fail with:

invalid_grant

Recovery Procedure (If invalid_grant Occurs)

Re-run bootstrap script:

python bootstrap_google_ads_oauth.py


Log in again via browser.

Copy the newly generated credential payload.

Update secure storage (Secret Manager / Vault / encrypted file).

Restart affected pipelines.

This bootstrap process must be repeated every time the refresh token is invalidated.

Production Storage Recommendation

The bootstrap script only generates credentials.

It is strongly recommended to:

Store credentials in:

Google Secret Manager

Vault

Encrypted configuration file

Never commit credentials to source control

Never share payload via chat/email

Recommended structure:

{
  "developer_token": "...",
  "client_id": "...",
  "client_secret": "...",
  "refresh_token": "...",
  "login_customer_id": "..."
}

Architecture Recommendation (Production)
Use a Dedicated Automation Account

For production systems:

Create a dedicated Google account for automation

Grant it Google Ads access (read-only if possible)

Use it exclusively for API access

Avoid using personal accounts

This prevents accidental revocation and access disruption.

Security Model Summary

Authentication method: OAuth (Desktop App flow)

Access pattern: Non-interactive via refresh token

Token rotation: Manual (via bootstrap script)

Service Accounts: Not supported by Google Ads API

Production usage: Supported via stored refresh token

Final Notes

The bootstrap script is a one-time setup under normal conditions.

If refresh token is revoked or expires, it must be regenerated.

Proper secret storage is critical for production stability.

Authentication errors are almost always related to refresh token invalidation, not API logic errors.