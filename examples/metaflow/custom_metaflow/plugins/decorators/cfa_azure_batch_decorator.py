from metaflow.decorators import StepDecorator
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import (
    ClientSecretCredential,
    ManagedIdentityCredential,
)

from cfa_azure.helpers import (
    add_job, 
    add_task_to_job,
    create_container,
    format_rel_path,
    get_batch_service_client, 
    get_sp_secret,
    read_config
)
from cfa_azure.batch_helpers import (
    create_batch_pool, 
    delete_pool,
    get_batch_mgmt_client, 
    get_pool_parameters
)
from cfa_azure.blob_helpers import (
    get_blob_config,
    get_blob_service_client,
    get_mount_config
)
from functools import wraps

DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"


class CFAAzureBatchDecorator(StepDecorator):
    """
    Specifies that this step should execute on Azure Batch.

    Parameters
    ----------
    config_file : str
        Path to the JSON configuration file containing Azure Batch settings.
    """

    name = "cfa_azure_batch"
    defaults = {
        'Authentication': None,
        'Batch': None,
        'Container': None,
        'Storage': None
    }

    def __init__(self, config_file=None, **kwargs):
        super(CFAAzureBatchDecorator, self).__init__(**kwargs)
        self.attributes = self.defaults.copy()
        # Load configuration from the JSON file if provided
        if config_file:
            self.attributes.update(read_config(config_file))

    def create_containers(self):
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

    def create_batch_pool(self):
        resource_group_name = self.attributes["Authentication"]["resource_group"]
        container_image_name = self.attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME)
        container_registry_server = self.attributes["Container"]["container_registry_server"]
        container_registry_url = self.attributes["Container"]["container_registry_url"]
        self._setup_secret_credentials()
        self.batch_client = get_batch_service_client(self.attributes, self.batch_cred)
        print("Azure Batch client setup complete.")
        self.create_containers()
        blob_config = []
        if self.mounts:
            for mount in self.mounts:
                blob_config.append(
                    get_blob_config(
                        mount[0], mount[1], True, self.attributes
                    )
                )
        self.mount_config = get_mount_config(blob_config)
        print("Azure Batch containers setup complete.")
        pool_parameters = get_pool_parameters(
            mode="autoscale",
            container_image_name=container_image_name,
            container_registry_url=container_registry_url,
            container_registry_server=container_registry_server,
            config=self.attributes,
            mount_config=self.mount_config,
            credential=self.secret_cred,
            use_default_autoscale_formula=True
        )
        self.batch_mgmt_client = get_batch_mgmt_client(config=self.attributes, credential=self.secret_cred)
        batch_json = {
            "account_name": self.attributes["Batch"]["batch_account_name"],
            "pool_id": self.attributes["Batch"]["pool_name"],
            "pool_parameters": pool_parameters,
            "resource_group_name": resource_group_name 
        }
        self.pool_id = create_batch_pool(self.batch_mgmt_client, batch_json)
        print(f'Azure batch pool {self.pool_id} was created')

    def _setup_secret_credentials(self):
        """
        Initialize the Azure Batch client using DefaultAzureCredential.
        """
        sp_secret = get_sp_secret(self.attributes, ManagedIdentityCredential())        
        self.secret_cred = ClientSecretCredential(
            tenant_id=self.attributes["Authentication"]["tenant_id"],
            client_id=self.attributes["Authentication"]["sp_application_id"],
            client_secret=sp_secret,
        )
        self.batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        )
        print("Secret credentials setup complete.")


    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_retries):
        """
        Perform cleanup after the task finishes.
        """    
        if hasattr(self, 'pool_id') and self.pool_id:
            print(f"Task {step_name} finished with status: {'OK' if is_task_ok else 'FAILED'}. Deleting batch pool {self.pool_id}.")    
            delete_pool(
                resource_group_name=self.attributes["Authentication"]["resource_group"],
                account_name=self.attributes["Batch"]["batch_account_name"],
                pool_name=self.pool_id,
                batch_mgmt_client=self.batch_mgmt_client
            )
            print(f"Batch pool {self.pool_id} has been deleted.")    

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not hasattr(self, 'pool_id'):
                self.create_batch_pool()
            job_id=self.attributes['Batch']['job_id']
            add_job(job_id=job_id, pool_id=self.pool_id, batch_client=self.batch_client, mark_complete=True)
            print("Azure Batch Job created")
            self.task_id = add_task_to_job(
                job_id=job_id, 
                task_id_base=f"{job_id}_task_", 
                docker_command="print 'hello'", 
                batch_client=self.batch_client, 
                full_container_name=DEFAULT_CONTAINER_IMAGE_NAME
            )
            return func(*args, **kwargs)
        return wrapper