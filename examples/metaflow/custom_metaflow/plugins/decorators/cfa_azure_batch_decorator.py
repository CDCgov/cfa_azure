import os
from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from metaflow.sidecar import Sidecar
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import (
    DefaultAzureCredential,
    ClientSecretCredential,
    ManagedIdentityCredential,
)
from azure.batch import BatchServiceClient
from azure.batch.models import PoolAddParameter, PoolInformation, JobAddParameter, TaskAddParameter

from cfa_azure.helpers import (
    add_job, 
    add_task_to_job,
    read_config, 
    get_batch_service_client, 
    get_sp_secret
)
from cfa_azure.batch_helpers import (
    create_batch_pool, 
    get_batch_mgmt_client, 
    get_pool_parameters
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
        'Container': None
    }

    def __init__(self, config_file=None, **kwargs):
        super(CFAAzureBatchDecorator, self).__init__(**kwargs)
        self.attributes = self.defaults.copy()
        # Load configuration from the JSON file if provided
        if config_file:
            self.attributes.update(read_config(config_file))

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        """
        Initialize the step with Azure Batch-specific settings.
        """
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore
        print("step_init")

        # Log the configuration
        print(f"Initializing Azure Batch for step: {step}")
        print(f"Azure Batch configuration: {self.attributes}")

    def runtime_init(self, flow, graph, package, run_id):
        """
        Initialize runtime-specific settings for Azure Batch.
        """
        print('Runtime init called.')
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

        # Setup Azure Batch client
        self._setup_batch_client()

    def create_batch_pool(self):
        self._setup_batch_client()
        
        print('Batch client setup complete.')

        resource_group_name = self.attributes["Authentication"]["resource_group"]
        container_image_name = self.attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME)
        container_registry_server = self.attributes["Container"]["container_registry_server"]
        container_registry_url = self.attributes["Container"]["container_registry_url"]
        self._setup_secret_credentials()
        pool_parameters = get_pool_parameters(
            mode="autoscale",
            container_image_name=container_image_name,
            container_registry_url=container_registry_url,
            container_registry_server=container_registry_server,
            config=self.attributes,
            mount_config=[],
            credential=self.secret_cred,
            use_default_autoscale_formula=True
        )
        batch_mgmt_client = get_batch_mgmt_client(config=self.attributes, credential=DefaultAzureCredential())
        batch_json = {
            "account_name": self.attributes["Batch"]["batch_account_name"],
            "pool_id": self.attributes["Batch"]["pool_name"],
            "pool_parameters": pool_parameters,
            "resource_group_name": resource_group_name 
        }
        self.pool_id = create_batch_pool(batch_mgmt_client, batch_json)

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
        print("Secret credentials setup complete.")

    def _setup_batch_client(self):
        """
        Initialize the Azure Batch client using DefaultAzureCredential.
        """
        self.credentials = DefaultAzureCredential()
        self.batch_client = BatchServiceClient(
            credentials=self.credentials,
            batch_url=self.attributes['Batch']['batch_service_url']
        )
        print("Azure Batch client setup complete.")

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        """
        Modify the CLI arguments to execute the step on Azure Batch.
        """
        print("runtime_step_cli")
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["azure_batch", "step"]
            cli_args.command_options.update(self.attributes)

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        """
        Perform pre-step initialization for Azure Batch.
        """
        self.metadata = metadata
        self.task_datastore = task_datastore
        print("task_pre_step")

        # Log Azure Batch metadata
        meta = {
            "azure-batch-job-id": os.getenv("AZURE_BATCH_JOB_ID", "unknown"),
            "azure-batch-job-attempt": os.getenv("AZURE_BATCH_JOB_ATTEMPT", "unknown"),
        }
        entries = [
            MetaDatum(
                field=k,
                value=v,
                type=k,
                tags=["attempt_id:{0}".format(retry_count)],
            )
            for k, v in meta.items()
        ]
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_retries):
        """
        Perform cleanup after the task finishes.
        """        
        print("task_finished")
        print(f"Task {step_name} finished with status: {'OK' if is_task_ok else 'FAILED'}")

    def __call__(self, func):
        # This makes the class behave like a decorator
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not hasattr(self, 'pool_id'):
                self.create_batch_pool()
            print("Using Azure Batch with config")
            return func(*args, **kwargs)
        return wrapper