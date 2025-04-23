# import modules for use
import csv
import datetime
import logging
import os
import subprocess as sp
import time
from zoneinfo import ZoneInfo as zi

import azure.batch.models as batchmodels
import docker
import numpy as np
import pandas as pd
import toml
import yaml
from azure.batch import BatchServiceClient
from azure.batch.models import (
    DependencyAction,
    ExitCodeMapping,
    ExitConditions,
    ExitOptions,
    JobAction,
    JobConstraints,
    MetadataItem,
    OnAllTasksComplete,
    OnTaskFailure,
    TaskConstraints,
)
from azure.containerregistry import ContainerRegistryClient
from azure.keyvault.secrets import SecretClient
from docker.errors import DockerException
from griddler import Griddle
from yaml import SafeLoader, dump, load

logger = logging.getLogger(__name__)


def read_config(config_path: str = "./configuration.toml") -> dict:
    """takes in a path to a configuration toml file and returns it as a json object

    Args:
        config_path (str): path to configuration toml file

    Returns:
        dict: json object with configuration info extracted from config file

    Example:
        config = read_config("/path/to/config.toml")
    """
    # print("Attempting to read configuration from:", config_path)
    try:
        config = toml.load(config_path)
        logger.debug("Configuration file loaded.")
        return config
    except FileNotFoundError as e:
        logger.warning(
            "Configuration file not found. Make sure the location (path) is correct."
        )
        logger.exception(e)
        raise FileNotFoundError(f"could not find file {config_path}") from None
    except Exception as e:
        logger.warning(
            "Error occurred while loading the configuration file. Check file format and contents."
        )
        logger.exception(e)
        raise Exception(
            "Error occurred while loading the configuration file. Check file format and contents."
        ) from None


def create_container(container_name: str, blob_service_client: object):
    """creates a Blob container if not exists

    Args:
        container_name (str): user specified name for Blob container
        blob_service_client (object): BlobServiceClient object

    Returns:
       object: ContainerClient object
    """
    logger.debug(f"Attempting to create or access container: {container_name}")
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    if not container_client.exists():
        container_client.create_container()
        logger.debug(f"Container [{container_name}] created successfully.")
    else:
        logger.debug(
            f"Container [{container_name}] already exists. No action needed."
        )
    return container_client


def get_sp_secret(config: dict, credential: object):
    """gets the user's secret from the keyvault based on config

    Args:
        config (dict): contains configuration info
        credential (object): credential object from azure.identity

    Returns:
        str: service principal secret

    Example:
        sp_secret = get_sp_secret(config, DefaultAzureClient())
    """

    logger.debug("Attempting to establish secret client.")
    try:
        secret_client = SecretClient(
            vault_url=config["Authentication"]["vault_url"],
            credential=credential,
        )
        logger.debug("Secret client initialized.")
    except KeyError as e:
        logger.error("Error:", e, "Key not found in configuration.")
        raise e

    logger.debug("Attempting to retrieve Service Principal secret.")
    try:
        sp_secret = secret_client.get_secret(
            config["Authentication"]["vault_sp_secret_id"]
        ).value
        logger.debug("Service principal secret successfully retrieved.")
        return sp_secret
    except Exception as e:
        logger.error("Error retrieving secret:", e)
        logger.warning(
            "Check that vault_uri and vault_sp_secret_id are correctly configured in the config file."
        )
        raise e


def get_batch_service_client(config: dict, credential: object):
    """creates and returns a Batch Service Client object

    Args:
        config (dict): config dictionary
        credential (object): credential object from azure.identity

    Returns:
        object: Batch Service Client object
    """
    # get batch credential
    logger.debug("Attempting to create Batch Service Client.")
    batch_client = BatchServiceClient(
        credentials=credential, batch_url=config["Batch"]["batch_service_url"]
    )
    logger.debug("Batch Service Client initialized successfully.")
    return batch_client


def list_nodes_by_pool(pool_name: str, config: dict, node_state: str = None):
    batch_client = get_batch_service_client(config)
    if node_state:
        filter_option = f"state eq '{node_state}'"
        nodes = batch_client.compute_node.list(
            pool_id=pool_name,
            compute_node_list_options=batchmodels.ComputeNodeListOptions(
                filter=filter_option
            ),
        )
    else:
        nodes = batch_client.compute_node.list(pool_id=pool_name)
    return nodes


def add_job(
    job_id: str,
    pool_id: str,
    batch_client: object,
    task_retries: int = 0,
    mark_complete: bool = False,
    timeout: int | None = None,
):
    """takes in a job ID and config to create a job in the pool

    Args:
        job_id (str): name of the job to run
        pool_id (str): name of pool
        batch_client (object): BatchClient object
        task_retries (int): number of times to retry the task if it fails. Default 3.
        mark_complete (bool): whether to mark the job complete after tasks finish running. Default False.
        timeout (int): timeout for job total runtime before forcing termination.
    """
    logger.debug(f"Attempting to create job '{job_id}'...")
    logger.debug("Adding job parameters to job.")
    on_all_tasks_complete = (
        OnAllTasksComplete.terminate_job
        if mark_complete
        else OnAllTasksComplete.no_action
    )
    if timeout is None:
        _to = None
    else:
        _to = datetime.timedelta(minutes=timeout)
    job_constraints = JobConstraints(
        max_task_retry_count=task_retries,
        max_wall_clock_time=_to,
    )

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id),
        uses_task_dependencies=True,
        on_all_tasks_complete=on_all_tasks_complete,
        on_task_failure=OnTaskFailure.perform_exit_options_job_action,
        constraints=job_constraints,
        metadata=[MetadataItem(name="mark_complete", value=mark_complete)],
    )
    logger.debug("Attempting to add job.")
    try:
        batch_client.job.add(job)
        logger.info(f"Job '{job_id}' created successfully.")
    except batchmodels.BatchErrorException as err:
        if err.error.code == "JobExists":
            logger.warning(
                f"Job '{job_id}' already exists. No further action taken."
            )
        else:
            logger.error(f"Error creating job '{job_id}': {err}")
            logger.error("Rename this job or delete the pre-existing job.")
            raise


def add_task_to_job(
    job_id: str,
    task_id_base: str,
    docker_command: str,
    save_logs_rel_path: str | None = None,
    logs_folder: str = "stdout_stderr",
    name_suffix: str = "",
    mounts: list | None = None,
    depends_on: str | list[str] | None = None,
    depends_on_range: tuple | None = None,
    run_dependent_tasks_on_fail: bool = False,
    batch_client: object | None = None,
    full_container_name: str | None = None,
    task_id_max: int = 0,
    task_id_ints: bool = False,
    timeout: int | None = None,
) -> str:
    """add a defined task(s) to a job in the pool

    Args:
        job_id (str): name given to job
        task_id_base (str): the name given to the task_id as a base
        docker_command (str): the docker command to execute for the task
        save_logs_rel_path (str): relative path to blob where logs should be stored. Default None for not saving logs.
        logs_folder (str): folder structure to save stdout logs to in blob container. Default is stdout_stderr.
        name_suffix (str): suffix to append to task name. Default is empty string.
        mounts (list[tuple]): a list of tuples in the form (container_name, relative_mount_directory)
        depends_on (str | list[str]): list of tasks this task depends on. Optional.
        depends_on_range (tuple): range of dependent tasks when task IDs are integers, given as (start_int, end_int). Optional.
        run_dependent_tasks_on_fail (bool): whether to run dependent tasks if the parent task fails. Default is False.
        batch_client (object): batch client object
        full_container_name (str): name ACR container to run task on
        task_id_max (int): current max task id in use by Batch
        task_id_ints (bool): whether to use incremental integers for task IDs. Optional.
        timeout (int): timeout in minutes before forcing task termination. Default None (infinity).
    Returns:
        str: task ID created
    """
    logger.debug("Adding add_task process.")
    # convert docker command to string if in list format
    if isinstance(docker_command, list):
        d_cmd_str = " ".join(docker_command)
        logger.debug("Docker command converted to string.")
    else:
        d_cmd_str = docker_command

    # Add a task to the job
    az_mount_dir = "$AZ_BATCH_NODE_MOUNTS_DIR"
    user_identity = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin,
        )
    )
    task_deps = None
    if depends_on is not None:
        # Create a TaskDependencies object to pass in
        if isinstance(depends_on, str):
            depends_on = [depends_on]
            logger.debug("Adding task dependency.")
        task_deps = batchmodels.TaskDependencies(task_ids=depends_on)
    if depends_on_range is not None:
        task_deps = batchmodels.TaskDependencies(
            task_id_ranges=[
                batchmodels.TaskIdRange(
                    start=int(depends_on_range[0]),
                    end=int(depends_on_range[1]),
                )
            ]
        )

    job_action = JobAction.none
    if check_job_exists(job_id, batch_client):
        job_details = batch_client.job.get(job_id)
        if job_details and job_details.metadata:
            for metadata in job_details.metadata:
                if (
                    metadata.name == "mark_complete"
                    and bool(metadata.value) is True
                ):
                    job_action = JobAction.terminate
                    break

    no_exit_options = ExitOptions(
        dependency_action=DependencyAction.satisfy, job_action=job_action
    )
    if run_dependent_tasks_on_fail:
        exit_conditions = ExitConditions(
            exit_codes=[
                ExitCodeMapping(code=0, exit_options=no_exit_options),
                ExitCodeMapping(code=1, exit_options=no_exit_options),
            ],
            pre_processing_error=no_exit_options,
            file_upload_error=no_exit_options,
            default=no_exit_options,
        )
    else:
        terminate_exit_options = ExitOptions(
            dependency_action=DependencyAction.block,
            job_action=job_action,
        )
        exit_conditions = ExitConditions(
            exit_codes=[
                ExitCodeMapping(code=0, exit_options=no_exit_options),
                ExitCodeMapping(code=1, exit_options=terminate_exit_options),
            ],
            pre_processing_error=terminate_exit_options,
            file_upload_error=terminate_exit_options,
            default=terminate_exit_options,
        )

    logger.debug("Creating mount configuration string.")
    mount_str = ""
    # src = env variable to fsmounts/rel_path
    # target = the directory(path) you reference in your code
    if mounts is not None:
        mount_str = ""
        for mount in mounts:
            logger.debug("Adding mount to mount string.")
            mount_str = (
                mount_str
                + "--mount type=bind,source="
                + az_mount_dir
                + f"/{mount[1]},target=/{mount[1]} "
            )
    if task_id_ints:
        task_id = str(task_id_max + 1)
    else:
        task_id = f"{task_id_base}-{name_suffix}-{str(task_id_max + 1)}"

    if save_logs_rel_path is not None:
        if save_logs_rel_path == "ERROR!":
            logger.warning("could not find rel path")
            print(
                "could not find rel path. Stdout and stderr will not be saved to blob storage."
            )
            full_cmd = d_cmd_str
        else:
            logger.debug("using rel path to save logs")
            t = datetime.datetime.now(zi("America/New_York"))
            s_time = t.strftime("%Y%m%d_%H%M%S")
            if not save_logs_rel_path.startswith("/"):
                save_logs_rel_path = "/" + save_logs_rel_path
            _folder = f"{save_logs_rel_path}/{logs_folder}/"
            sout = f"{_folder}/stdout_{job_id}_{task_id}_{s_time}.txt"
            full_cmd = f"""/bin/bash -c "mkdir -p {_folder}; {d_cmd_str} 2>&1 | tee {sout}" """
    else:
        full_cmd = d_cmd_str

    # add contstraints
    if timeout is None:
        _to = None
    else:
        _to = datetime.timedelta(minutes=timeout)
    task_constraints = TaskConstraints(max_wall_clock_time=_to)
    command_line = full_cmd
    logger.debug(f"Adding task {task_id}")
    task = batchmodels.TaskAddParameter(
        id=task_id,
        command_line=command_line,
        container_settings=batchmodels.TaskContainerSettings(
            image_name=full_container_name,
            container_run_options=f"--name={job_id}_{str(task_id_max+1)} --rm "
            + mount_str,
        ),
        user_identity=user_identity,
        depends_on=task_deps,
        exit_conditions=exit_conditions,
        constraints=task_constraints,
    )
    batch_client.task.add(job_id=job_id, task=task)
    logger.debug(f"Task '{task_id}' added to job '{job_id}'.")
    return task_id


def monitor_tasks(job_id: str, timeout: int, batch_client: object):
    """monitors tasks running in the job based on job ID

    Args:
        job_id (str): the name of the job to monitor
        timeout (int): number of minutes for timeout
        batch_client (object): an instance of batch client

    Raises:
        RuntimeError: this error is raised if the job does not complete in the timeout

    Returns:
        dict: dictionary with keys completed (whether the job completed) and runtime (total elapsed time)
    """
    # start monitoring
    logger.info(
        f"Starting to monitor tasks for job '{job_id}' with a timeout of {timeout} minutes."
    )

    start_time = datetime.datetime.now().replace(microsecond=0)
    if timeout is None:
        timeout = 480

    _timeout = datetime.timedelta(minutes=timeout)
    timeout_expiration = start_time + _timeout

    logger.debug(
        f"Job '{job_id}' monitoring started at {start_time}. Timeout at {timeout_expiration}."
    )
    logger.debug("-" * 20)

    # count tasks and print to user the starting value
    # as tasks complete, print which complete
    # print remaining number of tasks
    tasks = list(batch_client.task.list(job_id))

    # get total tasks
    total_tasks = len([task for task in tasks])
    logger.info(f"Total tasks to monitor: {total_tasks}")

    # pool setup and status
    # initialize job complete status
    completed = False
    job = batch_client.job.get(job_id)
    while job.as_dict()["state"] != "completed" or not completed:
        print(job.as_dict()['state'])
        print(completed)
        if datetime.datetime.now() < timeout_expiration:
            time.sleep(5)  # Polling interval
            tasks = list(batch_client.task.list(job_id))
            incomplete_tasks = [
                task
                for task in tasks
                if task.state != batchmodels.TaskState.completed
            ]
            incompletions = len(incomplete_tasks)
            completed_tasks = [
                task
                for task in tasks
                if task.state == batchmodels.TaskState.completed
            ]
            completions = len(completed_tasks)

            # initialize the counts
            failures = 0
            successes = 0

            for task in completed_tasks:
                if task.as_dict()["execution_info"]["result"] == "failure":
                    failures += 1
                elif task.as_dict()["execution_info"]["result"] == "success":
                    successes += 1

            print(
                completions,
                "completed;",
                incompletions,
                "remaining;",
                successes,
                "successes;",
                failures,
                "failures",
                end="\r",
            )
            logger.debug(
                f"{completions} completed; {incompletions} remaining; {successes} successes; {failures} failures"
            )

            if not incomplete_tasks:
                logger.info("\nAll tasks completed.")
                completed = True
                break
            job = batch_client.job.get(job_id)

    end_time = datetime.datetime.now().replace(microsecond=0)

    if completed:
        logger.info(
            "All tasks have reached 'Completed' state within the timeout period."
        )
        logger.info(f"{successes} task(s) succeeded, {failures} failed.")
    # get terminate reason
    if "terminate_reason" in job.as_dict()["execution_info"].keys():
        terminate_reason = job.as_dict()["execution_info"]["terminate_reason"]
    else:
        terminate_reason = None

    runtime = end_time - start_time
    logger.info(
        f"Monitoring ended: {end_time}. Total elapsed time: {runtime}."
    )
    return {
        "completed": completed,
        "elapsed time": runtime,
        "terminate_reason": terminate_reason,
    }


def df_to_yaml(df: pd.DataFrame):
    """converts a pandas dataframe to yaml

    Args:
        df (pd.DataFrame): pandas dataframe to convert to yaml

    Returns:
        dict:  yaml string converted from dataframe
    """
    logger.debug("Converting DataFrame to YAML format...")
    yaml_str = dump(
        df.to_dict(orient="records"), sort_keys=False, default_flow_style=False
    )
    logger.debug("Conversion complete.")
    return yaml_str


def yaml_to_df(yaml_file: dict):
    """converts a yaml file to pandas dataframe

    Args:
        yaml_file (dict): yaml file

    Returns:
        pd.DataFrame: pandas dataframe converted from yaml file
    """
    logger.debug("Converting YAML to DataFrame...")
    df = pd.json_normalize(load(yaml_file, Loader=SafeLoader))
    logger.debug("Conversion complete.")
    return df


def edit_yaml_r0(file_path: str, r0_start=1, r0_end=4, step=0.1):
    """takes in a yaml file and produces replicate yaml files with the r0 changed based on the start, stop, and step provided. Output yamls go to yaml/ folder.

    Args:
        file_path (str): path to file
        r0_start (int, optional): The lower end of the r0 range. Defaults to 1.
        r0_end (int, optional): The upped end of the r0 range (inclusive). Defaults to 4.
        step (float, optional): The step size of each r0 increase. Defaults to 0.1.
    """
    logger.debug(
        f"Starting to edit YAML file '{file_path}' with r0 range from {r0_start} to {r0_end} by steps of {step}."
    )

    with open(file_path, "r") as file:
        y = yaml.safe_load(file)
    logger.debug("Getting list or r0 values.")
    r0_list = np.arange(r0_start, r0_end + step, step, dtype=float).tolist()
    logger.debug("Looping through r0 values.")
    for r0 in r0_list:
        r0 = round(r0, len(str(step).split(".")[1]))
        y["baseScenario"]["r0"] = r0
        y["outputDirectory"] = os.path.join(y["outputDirectory"], str(r0))
        outfile = f"{file_path.replace('.yaml', '')}_{str(r0).replace('.', '-')}.yaml"
        with open(outfile, "w") as f:
            yaml.dump(y, f, default_flow_style=False)
        logger.debug(
            f"Generated modified YAML file with r0={r0} at '{outfile}'."
        )
    logger.debug("Completed editing YAML files.")


# check whether job exists
def check_job_exists(job_id: str, batch_client: object):
    """Checks whether a job exists.

    Args:
        job_id (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        bool: whether the job exists
    """
    job_list = batch_client.job.list()
    if job_id in job_list:
        logger.debug(f"job {job_id} exists.")
        return True
    else:
        logger.debug(f"job {job_id} does not exist.")
        return False


# check number of tasks completed
def get_completed_tasks(job_id: str, batch_client: object):
    """Return the number of completed tasks for the specified job.

    Args:
        job_id (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        dict: dictionary containing number of completed tasks and total tasks for the job
    """
    logger.debug("Pulling in task information.")
    tasks = batch_client.task.list(job_id)
    total_tasks = len(tasks)

    completed_tasks = [
        task for task in tasks if task.state == batchmodels.TaskState.completed
    ]
    num_c_tasks = len(completed_tasks)

    return {"completed tasks": num_c_tasks, "total tasks": total_tasks}


# check whether job is completed and open
def check_job_complete(job_id: str, batch_client: object) -> bool:
    """Checks if the job is complete.

    Args:
        job_id (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        bool: whether the specified job has completed
    """
    return get_job_state(job_id, batch_client) == "completed"


def get_job_state(job_id: str, batch_client: object):
    """returns the state of the specified job

    Args:
        job_id (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        str: the state of the specified job, such as 'completed' or 'active'.
    """
    job_info = batch_client.job.get(job_id)
    logger.debug(f"job state is {job_info.state}")
    return job_info.state


def package_and_upload_dockerfile(
    registry_name: str,
    repo_name: str,
    tag: str,
    path_to_dockerfile: str = "./Dockerfile",
    use_device_code: bool = False,
):
    """
    Packages Dockerfile in root of repo and uploads to the specified registry and repo with designated tag in Azure.

    Args:
        registry_name (str): name of Azure Container Registry
        repo_name (str): name of repo
        tag (str): tag for the Docker container
        path_to_dockerfile (str): path to Dockerfile. Default is ./Dockerfile.
        use_device_code (bool): whether to use the device code when authenticating. Default False.

    Returns:
        str: full container name
    """
    # check if Dockerfile exists
    logger.debug("Trying to ping docker daemon.")
    try:
        d = docker.from_env(timeout=10).ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not packaged/uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    if os.path.exists(path_to_dockerfile) and d:
        full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"
        logger.info(f"full container name: {full_container_name}")
        # Build container
        logger.debug("Building container.")
        sp.run(
            f"docker image build -f {path_to_dockerfile} -t {full_container_name} .",
            shell=True,
        )
        # Upload container to registry
        # upload with device login if desired
        if use_device_code:
            logger.debug("Logging in with device code.")
            sp.run("az login --use-device-code", shell=True)
        else:
            logger.debug("Logging in to Azure.")
            sp.run("az login", shell=True)
        sp.run(f"az acr login --name {registry_name}", shell=True)
        logger.debug("Pushing Docker container to ACR.")
        sp.run(f"docker push {full_container_name}", shell=True)
        return full_container_name
    else:
        logger.error("Dockerfile does not exist in the root of the directory.")
        raise Exception(
            "Dockerfile does not exist in the root of the directory."
        ) from None


def upload_docker_image(
    image_name: str,
    registry_name: str,
    repo_name: str,
    tag: str = "latest",
    use_device_code: bool = False,
):
    """
    Args:
        image_name (str): name of image in local Docker
        registry_name (str): name of Azure Container Registry
        repo_name (str): name of repo
        tag (str): tag for the Docker container. Default is "latest". If None, a timestamp tag will be generated.
        use_device_code (bool): whether to use the device code when authenticating. Default False.

    Returns:
        str: full container name
    """
    full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"

    # check if docker is running
    logger.debug("Trying to ping docker daemon.")
    try:
        docker_env = docker.from_env(timeout=8)
        docker_env.ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    # Tagging the image with the unique tag
    logger.debug(f"Tagging image {image_name} with {full_container_name}.")
    try:
        image = docker_env.images.get(image_name)
        image.tag(full_container_name)
    except docker.errors.ImageNotFound:
        # Log available images to guide the user
        available_images = [img.tags for img in docker_env.images.list()]
        logger.error(
            f"Image {image_name} does not exist. Available images are: {available_images}"
        )
        raise

    # Log in to ACR and upload container to registry
    # upload with device login if desired
    if use_device_code:
        logger.debug("Logging in with device code.")
        sp.run("az login --use-device-code", shell=True)
    else:
        logger.debug("Logging in to Azure.")
        sp.run("az login", shell=True)
    sp.run(f"az acr login --name {registry_name}", shell=True)
    logger.debug("Pushing Docker container to ACR.")
    sp.run(f"docker push {full_container_name}", shell=True)

    return full_container_name


def check_env_req() -> bool:
    """Checks if all necessary environment variables exist for the AzureClient.
    Returns true if all required variables are found, false otherwise.

    Returns:
        bool: true if environment variables contain all required components, false otherwise
    """
    config_to_env_var_map = {
        "Authentication.subscription_id": "AZURE_SUBSCRIPTION_ID",
        "Authentication.resource_group": "AZURE_RESOURCE_GROUP",
        "Authentication.user_assigned_identity": "AZURE_USER_ASSIGNED_IDENTITY",
        "Authentication.tenant_id": "AZURE_TENANT_ID",
        "Authentication.batch_application_id": "AZURE_BATCH_APPLICATION_ID",
        "Authentication.batch_object_id": "AZURE_BATCH_OBJECT_ID",
        "Authentication.sp_application_id": "AZURE_SP_APPLICATION_ID",
        "Authentication.vault_url": "AZURE_VAULT_URL",
        "Authentication.vault_sp_secret_id": "AZURE_VAULT_SP_SECRET_ID",
        "Authentication.subnet_id": "AZURE_SUBNET_ID",
        "Batch.batch_account_name": "AZURE_BATCH_ACCOUNT_NAME",
        "Batch.batch_service_url": "AZURE_BATCH_SERVICE_URL",
        "Batch.pool_vm_size": "AZURE_POOL_VM_SIZE",
        "Storage.storage_account_name": "AZURE_STORAGE_ACCOUNT_NAME",
        "Storage.storage_account_url": "AZURE_STORAGE_ACCOUNT_URL",
    }
    missing_vars = [
        env_var
        for env_var in config_to_env_var_map.values()
        if not os.getenv(env_var)
    ]

    if not missing_vars:
        logger.debug("All required environment variables are set.")
    else:
        logger.warning(f"Missing environment variables: {missing_vars}")
    return missing_vars


def check_config_req(config: str):
    """checks if the config file has all the necessary components for the client
    Returns true if all components exist in config.
    Returns false if not.

    Args:
        config (str): config dict

    Returns:
        bool: true if config contains all required components, false otherwise
    """

    req = set(
        [
            "Authentication.subscription_id",
            "Authentication.resource_group",
            "Authentication.user_assigned_identity",
            "Authentication.tenant_id",
            "Authentication.batch_application_id",
            "Authentication.batch_object_id",
            "Authentication.sp_application_id",
            "Authentication.vault_url",
            "Authentication.vault_sp_secret_id",
            "Authentication.subnet_id",
            "Batch.batch_account_name",
            "Batch.batch_service_url",
            "Batch.pool_vm_size",
            "Storage.storage_account_name",
            "Storage.storage_account_url",
        ]
    )
    logger.debug("Loading config info as a set.")
    loaded = set(pd.json_normalize(config).columns)
    logger.debug("Comparing keys in config and the list of required keys.")
    check = req - loaded == set()
    if check:
        logger.debug("All required keys exist in the config.")
        return None
    else:
        missing = str(list(req - loaded))
        logger.warning(
            "%s missing from the config file and may be required by client.",
            missing,
        )
        return missing


def get_container_registry_client(
    endpoint: str, credential: object, audience: str
):
    """
    Args:
        endpoint (str): the endpoint to the container registry
        credential (object): a credential object
        audience (str): audience for container registry client
    """
    return ContainerRegistryClient(endpoint, credential, audience=audience)


def check_azure_container_exists(
    registry_name: str, repo_name: str, tag_name: str, credential: object
) -> str:
    """specify the container in ACR to use without packaging and uploading the docker container from local.

    Args:
        registry_name (str): the name of the registry in Azure Container Registry
        repo_name (str): the name of the repo
        tag_name (str): the tag name
        credential (object): credential object from azure.identity

    Returns:
        str: full name of container
    """
    # check full_container_name exists in ACR
    audience = "https://management.azure.com"
    endpoint = f"https://{registry_name}.azurecr.io"
    logger.debug(f"Set audience to {audience}")
    logger.debug(f"Set endpoint to {endpoint}")
    try:
        # check full_container_name exists in ACR
        cr_client = get_container_registry_client(
            endpoint=endpoint, credential=credential, audience=audience
        )
        logger.debug("Container registry client created. Container exists.")
    except Exception as e:
        logger.error(
            f"Registry [{registry_name}] or repo [{repo_name}] does not exist"
        )
        logger.exception(e)
        raise e
    tag_list = []
    logger.debug("Checking tag exists.")
    for tag in cr_client.list_tag_properties(repo_name):
        tag_properties = cr_client.get_tag_properties(repo_name, tag.name)
        tag_list.append(tag_properties.name)
    logger.debug(f"Available tags in repo: {tag_list}")
    if tag_name in tag_list:
        logger.debug(f"setting {registry_name}/{repo_name}:{tag_name}")
        full_container_name = (
            f"{registry_name}.azurecr.io/{repo_name}:{tag_name}"
        )
        return full_container_name
    else:
        logger.warning(
            f"{registry_name}/{repo_name}:{tag_name} does not exist"
        )
        return None


def format_rel_path(rel_path: str) -> str:
    """
    Formats a relative path into the right format for Azure services

    Args:
        rel_path (str): relative mount path

    Returns:
        str: formatted relative path
    """
    if rel_path.startswith("/"):
        rel_path = rel_path[1:]
        logger.debug(f"path formatted to {rel_path}")
    return rel_path


def get_timeout(_time: str) -> int:
    """
    Args:
        _time (str): formatted timeout string

    Returns:
        int: integer of timeout in minutes
    """
    t = _time.split("PT")[-1]
    if "H" in t:
        if "M" in t:
            h = int(t.split("H")[0])
            m = int(t.split("H")[1].split("M")[0])
            return 60 * h + m
        else:
            m = int(t.split("H")[0])
            return m * 60
    else:
        m = int(t.split("M")[0])
        return m


def get_log_level() -> int:
    """
    Gets the LOG_LEVEL from the environment.

    If it could not find one, set it to None.

    If one was found, but not expected, set it to DEBUG
    """
    log_level = os.getenv("LOG_LEVEL")

    if log_level is None:
        return logging.CRITICAL + 1

    match log_level.lower():
        case "none":
            return logging.CRITICAL + 1
        case "debug":
            logger.info("Log level set to DEBUG")
            return logging.DEBUG
        case "info":
            logger.info("Log level set to INFO")
            return logging.INFO
        case "warning" | "warn":
            logger.info("Log level set to WARNING")
            return logging.WARNING
        case "error":
            logger.info("Log level set to ERROR")
            return logging.ERROR
        case "critical":
            logger.info("Log level set to CRITICAL")
            return logging.CRITICAL
        case ll:
            logger.warning(
                f"Did not recognize log level string {ll}. Using DEBUG"
            )
            return logging.DEBUG


def check_autoscale_parameters(
    mode: str,
    dedicated_nodes: int = None,
    low_priority_nodes: int = None,
    node_deallocation_option: int = None,
    autoscale_formula_path: str = None,
    evaluation_interval: str = None,
) -> str | None:
    """Checks which arguments are incompatible with the provided scale mode

    Args:
        mode (str): pool mode, chosen from 'fixed' or 'autoscale'
        dedicated_nodes (int): optional, the target number of dedicated compute nodes for the pool in fixed scaling mode. Defaults to None.
        low_priority_nodes (int): optional, the target number of spot compute nodes for the pool in fixed scaling mode. Defaults to None.
        node_deallocation_option (str): optional, determines what to do with a node and its running tasks after it has been selected for deallocation. Defaults to None.
        autoscale_formula_path (str): optional, path to autoscale formula file if mode is autoscale. Defaults to None.
        evaluation_interval (str): optional, how often Batch service should adjust pool size according to its autoscale formula. Defaults to 15 minutes.
    """
    if mode == "autoscale":
        disallowed_args = [
            {"arg": dedicated_nodes, "label": "dedicated_nodes"},
            {"arg": low_priority_nodes, "label": "low_priority_nodes"},
            {
                "arg": node_deallocation_option,
                "label": "node_deallocation_option",
            },
        ]
    else:
        disallowed_args = [
            {"arg": autoscale_formula_path, "label": "autoscale_formula_path"},
            {"arg": evaluation_interval, "label": "evaluation_interval"},
        ]
    validation_errors = [
        d_arg["label"] for d_arg in disallowed_args if d_arg["arg"]
    ]
    if validation_errors:
        invalid_fields = ", ".join(validation_errors)
        validation_msg = (
            f"{invalid_fields} cannot be specified with {mode} option"
        )
        return validation_msg
    return None


def download_job_stats(
    job_id: str, batch_service_client: object, file_name: str | None = None
):
    if file_name is None:
        file_name = f"{job_id}-stats"
    r = batch_service_client.task.list(
        job_id=job_id,
    )

    fields = [
        "task_id",
        "command",
        "creation",
        "start",
        "end",
        "runtime",
        "exit_code",
        "pool",
        "node_id",
    ]
    with open(rf"{file_name}.csv", "w") as f:
        logger.debug(f"initializing {file_name}.csv.")
        writer = csv.writer(f, delimiter="|")
        writer.writerow(fields)
    for item in r:
        st = item.execution_info.start_time
        et = item.execution_info.end_time
        rt = et - st
        id = item.id
        creation = item.creation_time
        start = item.execution_info.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end = item.execution_info.end_time.strftime("%Y-%m-%d %H:%M:%S")
        exit_code = item.execution_info.exit_code
        node_id = item.node_info.node_id
        cli = item.command_line.split(" -")[0]
        pool = item.node_info.pool_id
        fields = [id, cli, creation, start, end, rt, exit_code, pool, node_id]
        with open(rf"{file_name}.csv", "a") as f:
            writer = csv.writer(f, delimiter="|")
            writer.writerow(fields)
            logger.debug("wrote task to job statistic csv.")
    print(f"Downloaded job statistics report to {file_name}.csv.")


def get_args_from_yaml(file_path: str) -> list[str]:
    """
    parses yaml file and returns list of strings containing command line arguments and flags captured in the yaml.

    Args:
        file_path (str): path to yaml file

    Returns:
        list[str]: list of command line arguments
    """
    with open(file_path) as f:
        raw_griddle = yaml.safe_load(f)
    griddle = Griddle(raw_griddle)
    parameter_sets = griddle.parse()
    output = []
    for i in parameter_sets:
        full_cmd = ""
        for key, value in i.items():
            if key.endswith("(flag)"):
                if value != "":
                    full_cmd += f""" --{key.split("(flag)")[0]}"""
            else:
                full_cmd += f" --{key} {value}"
        output.append(full_cmd)
    return output


def get_tasks_from_yaml(base_cmd: str, file_path: str) -> list[str]:
    """
    combines output of get_args_from_yaml with a base command to get a complete command

    Args:
        base_cmd (str): base command to append the rest of the yaml arguments to
        file_path (str): path to yaml file

    Returns:
        list[str]: list of full commands created by joining the base command with each set of parameters
    """
    cmds = []
    arg_list = get_args_from_yaml(file_path)
    for s in arg_list:
        cmds.append(f"{base_cmd} {s}")
    return cmds
