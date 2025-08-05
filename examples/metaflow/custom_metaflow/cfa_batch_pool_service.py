from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import (
    ClientSecretCredential,
    ManagedIdentityCredential,
)

from cfa_azure.helpers import (
    get_sp_secret
)

from cfa_azure.helpers import (
    get_batch_service_client 
)
from cfa_azure.batch_helpers import (
    get_batch_mgmt_client
)
from cfa_azure.batch_helpers import (
    check_pool_exists,
    create_batch_pool, 
    delete_pool,
    get_pool_parameters
)
from cfa_azure.blob_helpers import (
    get_blob_config,
    get_blob_service_client,
    get_mount_config
)
from cfa_azure.helpers import (
    create_container,
    format_rel_path
)

class CFABatchPoolService:
    def __init__(self):
        self.batch_mgmt_client = None
        self.batch_client = None
        self.batch_cred = None
        self.secret_cred = None
        self.batch_pools = {}

    def has_secret_credentials(self) -> bool:
        """
        Check if the service has secret credentials.

        Returns:
            bool: True if secret credentials are available, False otherwise.
        """
        return hasattr(self, 'secret_credentials') and self.secret_credentials is not None
    
    def setup_secret_credentials(self, attributes):
        sp_secret = get_sp_secret(self.attributes, ManagedIdentityCredential())        
        self.secret_cred = ClientSecretCredential(
            tenant_id=attributes["tenant_id"],
            client_id=attributes["Authentication"]["sp_application_id"],
            client_secret=sp_secret,
        )
        self.batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        )
        print("Secret credentials setup complete.")

    def setup_clients(self, attributes):
        self.batch_client = get_batch_service_client(attributes, self.batch_cred)
        self.batch_mgmt_client = get_batch_mgmt_client(config=attributes, credential=self.secret_cred)

    def _create_containers(self):
        self.mounts = []
        self.mount_container_clients = []
        self.blob_service_client = get_blob_service_client(
            self.attributes, self.secret_cred
        )
        container_names = ['input', 'output']
        for name in container_names:
            rel_mount_dir = format_rel_path(f"/{name}")
            self.mounts.append((f"cfa{name}", rel_mount_dir))
            create_container(f"cfa{name}", self.blob_service_client)

    def fetch_or_create_pool(self, pool_name, attributes) -> bool:
        resource_group_name = attributes["Authentication"]["resource_group"]
        account_name = attributes["Batch"]["batch_account_name"]
        if pool_name in self.batch_pools.keys() or check_pool_exists(resource_group_name, account_name, pool_name, self.batch_mgmt_client):
            self.pool_id = pool_name
            self.batch_pools[pool_name] = {'container_created': True}
            print(f'Existing Azure batch pool {self.pool_id} is being reused')           
        else:
            self._create_containers()
            blob_config = []
            if self.mounts:
                for mount in self.mounts:
                    blob_config.append(
                        get_blob_config(
                            mount[0], mount[1], True, self.attributes
                        )
                    )
            self.mount_config = get_mount_config(blob_config)
            pool_parameters = get_pool_parameters(
                mode="autoscale",
                container_image_name=self.attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME),
                container_registry_url=self.attributes["Container"]["container_registry_url"],
                container_registry_server=self.attributes["Container"]["container_registry_server"],
                config=self.attributes,
                mount_config=self.mount_config,
                credential=self.secret_cred,
                use_default_autoscale_formula=True
            )
            batch_json = {
                "account_name": self.attributes["Batch"]["batch_account_name"],
                "pool_id": pool_name,
                "pool_parameters": pool_parameters,
                "resource_group_name": self.attributes["Authentication"]["resource_group"] 
            }
            self.pool_id = create_batch_pool(self.batch_mgmt_client, batch_json)
            self.batch_pools[self.pool_id] = {'container_created': True}
        return self.pool_id

    def delete_all_pools(self):
        for pool_name, _ in self.batch_pools.items():
            resource_group_name = self.attributes["Authentication"]["resource_group"]
            account_name = self.attributes["Batch"]["batch_account_name"]
            delete_pool(resource_group_name, account_name, pool_name, self.batch_mgmt_client)
            print(f"Deleted Azure Batch Pool: {pool_name}")