from google.cloud import secretmanager
import json

class InternalGCPSecretManager:
    def __init__(self, project_id: str):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id

    def get_secret(self, secret_id: str, version: str = "latest") -> dict:
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
        response = self.client.access_secret_version(name=name)
        return json.loads(response.payload.data.decode("UTF-8"))