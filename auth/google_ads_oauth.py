import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import json

from google_auth_oauthlib.flow import InstalledAppFlow

# Default Google Ads scopes
SCOPES = ["https://www.googleapis.com/auth/adwords"]

# Put your OAuth client secret desktop app location here
GCP_OAUTH2_CLIENT_SECRET_FILE = Path(__file__).resolve().parent / "oauth2_desktop_client.json"

# Put your Google Ads MCC Developer Token here
GOOGLE_ADS_MCC_DEVELOPER_TOKEN = "PUT-YOUR-GOOGLE-ADS-DEVELOPER-TOKEN-HERE"

# Put your Google Ads MCC numbers only no dashes customer ID here
GOOGLE_ADS_MCC_CUSTOMER_ID = "PUT-YOUR-GOOGLE-ADS-MCC-NUMBERS-WITH-NO-DASHES-HERE"

def internal_bootstrap_oauth():
    """
    Google Ads OAuth Bootstrap
    ---
    Principles:
        1. Initialize OAuth2 flow using Desktop Client Secret configuration
        2. Request user consent for Google Ads API scope
        3. Generate offline access credentials with refresh_token
        4. Construct Google Ads credential payload
        5. Output secure credential payload
    ---
    Returns:
        1. dict:
            Credential payload containing developer_token, client_id,
            client_secret, refresh_token, and login_customer_id
    """    
    
    flow = InstalledAppFlow.from_client_secrets_file(
        GCP_OAUTH2_CLIENT_SECRET_FILE,
        scopes=SCOPES
    )

    credentials = flow.run_local_server(
        port=0,
        prompt="consent",
        access_type="offline"
    )

    payload = {
        "developer_token": GOOGLE_ADS_MCC_DEVELOPER_TOKEN,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "refresh_token": credentials.refresh_token,
        "login_customer_id": GOOGLE_ADS_MCC_CUSTOMER_ID,
    }

    print("\n" + "=" * 64)
    print(" Google Ads OAuth Bootstrap")
    print("=" * 64)

    print("\nSuccessfully generated credential payload:\n")
    print(json.dumps(payload, indent=2))

    print("\n" + "-" * 64)
    print("SECURITY WARNING")
    print("- This output contains sensitive credentials")
    print("- Run this script only on a trusted local machine")
    print("- Do NOT commit this payload to source control")
    print("- Do NOT share this payload over chat or email")

    print("\nNEXT STEPS")
    print("1. Copy the credential payload above")
    print("2. Store it securely with Secret Manager/Vault/Encrypted file")
    print("3. Use it to initialize GoogleAdsClient")
    print("4. This OAuth bootstrap only needs to be run only ONCE")

    print("\n" + "=" * 64 + "\n")

    return payload

if __name__ == "__main__":
    
    internal_bootstrap_oauth()