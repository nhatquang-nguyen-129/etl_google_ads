from google.cloud import secretmanager
import json

class GCPSecretManager:

    def __init__(
            self, 
            project_id: str
            ):
        
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id

    def _secret_path(
            self, 
            secret_id: str, 
            version: str = "latest"
            ):
        
        return f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"

    def get_secret(
            self, 
            secret_id: str
            ) -> dict:
        
        response = self.client.access_secret_version(
            name=self._secret_path(secret_id)
        )
        payload = response.payload.data.decode("utf-8")
        return json.loads(payload)