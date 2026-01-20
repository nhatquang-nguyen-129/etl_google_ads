from google.ads.googleads.client import GoogleAdsClient
from auth.google_ads.secret_manager import GCPSecretManager


class internalGoogleAdsClient:

    def __init__(
        self,
        project_id: str,
        secret_id: str
    ):
        self.secret_manager = GCPSecretManager(project_id)
        self.secret_id = secret_id

    def get_client(self) -> GoogleAdsClient:
        secret = self.secret_manager.get_secret(self.secret_id)

        config = {
            "developer_token": secret["developer_token"],
            "client_id": secret["client_id"],
            "client_secret": secret["client_secret"],
            "refresh_token": secret["refresh_token"],
            "use_proto_plus": True,
        }

        # MCC optional
        if "login_customer_id" in secret:
            config["login_customer_id"] = secret["login_customer_id"]

        return GoogleAdsClient.load_from_dict(config)
