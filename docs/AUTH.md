# Google Ads Bootstrap for Authentication

## Purpose

- Generate a long-lived **refresh token** via **one-time OAuth login** via browser

- Generate a reusable Google Ads credentials payload which only needs to be run only **ONCE**

- Enable **non-interactive** access in production environments

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

- Fill required Google Ads MCC Developer Token variable
```text
GOOGLE_ADS_MCC_DEVELOPER_TOKEN = "PUT_YOUR_GOOGLE_ADS_MCC_DEVELOPER_TOKEN_HERE"
```

- Fill required Google Ads MCC customer ID variable
```text
GOOGLE_ADS_MCC_CUSTOMER_ID = "PUT_YOUR_GOOGLE_ADS_MCC_CUSTOMER_ID_HERE"
```

- Run `google_ads_oauth.py` to get assembled single payload credentials
```bash
python auth/google_ads_oauth.py
```

- Script opens browser for OAuth consent and example console output below:
```text
Successfully generated credential payload:
{
  "developer_token": "AIzaSyD-EXAMPLE-DEVELOPER-TOKEN",
  "client_id": "1234567890-abcdefg.apps.googleusercontent.com",
  "client_secret": "GOCSPX-EXAMPLE-CLIENT-SECRET",
  "refresh_token": "1//0gEXAMPLE_REFRESH_TOKEN_LONG_STRING",
  "login_customer_id": "1234567890"
}
```

- Copy the credential payload above then store it securely with `Secret Manager

- Use the credential payload to initialize `GoogleAdsClient`

## Revoke

### Use cases

- Google Ads will **invalidate refresh token** if access from OAuth Desktop App has been revoked

- Google Ads will **invalidate refresh token** if OAuth client ID has been changed or deleted

- Google Ads will **invalidate refresh token** if OAuth consent screen has been modified significantly

- Google Ads will **invalidate refresh token** if Google account password has been reset

- Google Ads will **invalidate refresh token** if this token is unused for 6 months

### Re-run Bootstrap Authentication

- Re-run `google_ads_oauth.py` to get assembled single payload credentials
```bash
python google_ads_oauth.py
```

- Script opens browser for OAuth consent and example console output below:
```text
Successfully generated credential payload:
{
  "developer_token": "AIzaSyD-EXAMPLE-DEVELOPER-TOKEN",
  "client_id": "1234567890-abcdefg.apps.googleusercontent.com",
  "client_secret": "GOCSPX-EXAMPLE-CLIENT-SECRET",
  "refresh_token": "1//0gEXAMPLE_REFRESH_TOKEN_LONG_STRING",
  "login_customer_id": "1234567890"
}
```

- Copy the newly generated credential payload above then update new version with Secret Manager

- Use the credential payload to initialize `GoogleAdsClient`