from cfa_azure.batch_helpers import (
    check_pool_exists,
    create_batch_pool, 
    delete_pool,
    get_pool_parameters
)
from cfa_azure.blob_helpers import (
    get_blob_config,
    get_mount_config
)
from cfa_azure.helpers import (
    create_container,
    format_rel_path
)
DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"

class CFABatchPoolService:
    def __init__(self):
        self.batch_pools = {}
        self.resource_group_name = None
        self.account_name = None

    def _create_containers(self, blob_service_client) -> list:
        mounts = []
        container_names = ['input', 'output']
        for name in container_names:
            rel_mount_dir = format_rel_path(f"/{name}")
            mounts.append((f"cfa{name}", rel_mount_dir))
            create_container(f"cfa{name}", blob_service_client)
        return mounts

    def fetch_or_create_pool(self, pool_name, attributes, batch_mgmt_client, blob_service_client) -> bool:
        self.resource_group_name = attributes["Authentication"]["resource_group"]
        self.account_name = attributes["Batch"]["batch_account_name"]
        if pool_name in self.batch_pools.keys() or check_pool_exists(self.resource_group_name, self.account_name, pool_name, batch_mgmt_client):
            pool_id = pool_name
            self.batch_pools[pool_name] = {'container_created': True}
            print(f'Existing Azure batch pool {self.pool_id} is being reused')           
        else:
            mounts = self._create_containers(blob_service_client)
            blob_config = []
            if mounts:
                for mount in mounts:
                    blob_config.append(
                        get_blob_config(
                            mount[0], mount[1], True, attributes
                        )
                    )
            mount_config = get_mount_config(blob_config)
            pool_parameters = get_pool_parameters(
                mode="autoscale",
                container_image_name=attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME),
                container_registry_url=attributes["Container"]["container_registry_url"],
                container_registry_server=attributes["Container"]["container_registry_server"],
                config=attributes,
                mount_config=mount_config,
                credential=secret_cred,
                use_default_autoscale_formula=True
            )
            batch_json = {
                "account_name": attributes["Batch"]["batch_account_name"],
                "pool_id": pool_name,
                "pool_parameters": pool_parameters,
                "resource_group_name": attributes["Authentication"]["resource_group"] 
            }
            pool_id = create_batch_pool(batch_mgmt_client, batch_json)
            self.batch_pools[pool_id] = {'container_created': True}
        return pool_id

    def delete_all_pools(self):
        #for pool_name, _ in self.batch_pools.items():
            #delete_pool(self.resource_group_name, self.account_name, pool_name, self.batch_mgmt_client)
            #print(f"Deleted Azure Batch Pool: {pool_name}")
        return True