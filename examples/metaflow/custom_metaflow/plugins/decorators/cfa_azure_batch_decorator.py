import os
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

    #defaults = {
    #    "cpu": 1,
    #    "gpu": 0,
    #    "memory": 4096,
    #    "image": "python:3.9-slim",
    #    "queue": None,
    #    "resource_group": None,
    #    "batch_url": None,
    #    "container_registry_url": None,
    #    "container_image_name": None,
    #    "timeout_seconds": 3600,
    #}

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
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        # Log the configuration
        self.logger(f"Initializing Azure Batch for step: {step}")
        self.logger(f"Azure Batch configuration: {self.attributes}")

    def runtime_init(self, flow, graph, package, run_id):
        """
        Initialize runtime-specific settings for Azure Batch.
        """
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

        # Setup Azure Batch client
        self._setup_batch_client()

    def _setup_batch_client(self):
        """
        Initialize the Azure Batch client using DefaultAzureCredential.
        """
        self.credentials = DefaultAzureCredential()
        self.batch_client = BatchServiceClient(
            credential=self.credentials,
            batch_url=self.attributes["batch_url"]
        )
        self.logger("Azure Batch client setup complete.")

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
        self.logger(f"Task {step_name} finished with status: {'OK' if is_task_ok else 'FAILED'}")

    def __call__(self, func):
        # This makes the class behave like a decorator
        @wraps(func)
        def wrapper(*args, **kwargs):
            print("Using Azure Batch with config")
            return func(*args, **kwargs)
        return wrapper