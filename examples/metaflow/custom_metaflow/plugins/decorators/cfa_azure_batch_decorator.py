import os
import json
from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum
from metaflow.sidecar import Sidecar
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import (
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.batch import BatchServiceClient

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

DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"

class CFAAzureBatchDecorator(StepDecorator):
    name = "cfa_azure_batch"
    """
    Specifies that this step should execute on Azure Batch.

    Parameters
    ----------
    config_file : str
        Path to the JSON configuration file containing Azure Batch settings.
    """

    name = "cfa_azure_batch"
    defaults = {
        "cpu": 1,
        "gpu": 0,
        "memory": 4096,
        "image": "python:3.9-slim",
        "queue": None,
        "iam_role": None,
        "execution_role": None,
        "timeout_seconds": 3600,
    }

    def __init__(self, config_file=None, **kwargs):
        self.attributes = read_config(config_file)
        self.attributes = self.defaults.copy()
        super(CFAAzureBatchDecorator, self).__init__(self.attributes)

        # Load configuration from the client-config.json file if provided
        if self.config_file:
            self.attributes.update(self._read_config(self.config_file))

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        """
        Initialize the step with Azure Batch-specific settings.
        """
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        # Set runtime attributes
        self.attributes["timeout_seconds"] = get_run_time_limit_for_task(decos)

        # Log the configuration
        self.logger(f"Initializing Azure Batch for step: {step}")
        self.logger(f"Azure Batch configuration: {self.attributes}")

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        """
        Modify the CLI arguments to execute the step on Azure Batch.
        """
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

        # Log Azure Batch metadata
        meta = {
            "azure-batch-job-id": os.environ.get("AZURE_BATCH_JOB_ID", "unknown"),
            "azure-batch-job-attempt": os.environ.get("AZURE_BATCH_JOB_ATTEMPT", "unknown"),
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

        # Start a sidecar for monitoring logs (if applicable)
        self._log_sidecar = Sidecar("azure_batch_log_monitor")
        self._log_sidecar.start()

    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_retries):
        """
        Perform cleanup after the task finishes.
        """
        try:
            self._log_sidecar.terminate()
        except:
            pass

        # Sync metadata to the datastore if necessary
        if hasattr(self, "metadata") and self.metadata.TYPE == "local":
            sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, self.task_datastore)


    def setup_batch_management_client(self):
        self.credentials = DefaultAzureCredential()
        self.batch_client = BatchServiceClient(
            credential=self.credentials,
            batch_url=os.getenv("AZURE_BATCH_URL")
        )
        print('Batch client setup complete.')

        resource_group_name = self.attributes["Authentication"]["resource_group"]
        container_image_name = self.attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME)
        container_registry_server = self.attributes["Container"]["container_registry_server"]
        container_registry_url = self.attributes["Container"]["container_registry_url"]
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
        batch_mgmt_client = get_batch_mgmt_client(config=self.attributes, credential=DefaultAzureCredential())
        batch_json = {
            "account_name": self.attributes["Batch"]["batch_account_name"],
            "pool_id": self.attributes["Batch"]["pool_name"],
            "pool_parameters": pool_parameters,
            "resource_group_name": resource_group_name 
        }
        create_batch_pool(batch_mgmt_client, batch_json)


    def submit_task_to_azure_batch(self, step_run):
        sp_secret = get_sp_secret(
            self.attributes, ManagedIdentityCredential()
        )
        batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        )
        batch_service_client = get_batch_service_client(
            self.attributes, batch_cred
        )
        job_id = "my_job_id"
        add_job(job_id, self.attributes["Batch"]["pool_name"], batch_service_client)
        add_task_to_job(job_id=job_id, task_id_base=job_id, docker_command="python app.py")

    def _apply(self, step_name, flow, graph, decos, environment, datastore, logger):
        print(f"Applying AzureBatchDecorator to step: {step_name}")
        self.setup_batch_management_client()
        self.submit_task_to_azure_batch(step.run)
        step.run = self._wrap_step_run(step.run)