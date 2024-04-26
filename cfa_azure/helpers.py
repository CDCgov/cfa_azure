# import modules for use
import datetime
import json
import os
import subprocess as sp
import sys
import time
from pathlib import Path

import azure.batch.models as batchmodels
import docker
import numpy as np
import pandas as pd
import toml
import yaml
from azure.batch import BatchServiceClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.containerregistry import ContainerRegistryClient
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.batch import BatchManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from docker.errors import DockerException
from yaml import SafeLoader, dump, load


def read_config(config_path: str = "./configuration.toml"):
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
        # print("Configuration file loaded successfully.")
        return config
    except FileNotFoundError as e:
        print(
            "Configuration file not found. Make sure the location (path) is correct."
        )
        print(e)
    except Exception as e:
        print(
            "Error occurred while loading the configuration file. Check file format and contents."
        )
        print(e)


def create_container(container_name: str, blob_service_client: object):
    """creates a Blob container if not exists

    Args:
        container_name (str): user specified name for Blob container
        blob_service_client (object): BlobServiceClient object

    Returns:
       object: ContainerClient object
    """
    print(f"Attempting to create or access container: {container_name}")
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    if not container_client.exists():
        container_client.create_container()
        print(f"Container [{container_name}] created successfully.")
    else:
        print(
            f"Container [{container_name}] already exists. No action needed."
        )
    return container_client


def get_autoscale_formula(filepath: str = None, text_input: str = None):
    """takes in a file to autoscale file or a "static" text input, reads and returns an autoscale formula.
    If neither are found, it will look for a file named autoscale_formula.txt and return its string output.

    Args:
        filepath (str): a path to an autoscale formula file. Default is None.
        text_input (str): a string input of the autoscale formula. Default is None.
    Returns:
        str: autoscale formula
    """
    # print("Retrieving autoscale formula...")
    if filepath is None and text_input is None:
        #get default autoscale formula:
        autoscale_text = generate_autoscale_formula()
        print(
            "Default autoscale formula used. Please provide a path to autoscale formula to sepcify your own formula."
        )
        return autoscale_test
    elif filepath is not None:
        try:
            with open(filepath, "r") as autoscale_text:
                print(f"Autoscale formula successfully read from {filepath}.")
                return autoscale_text.read()
        except Exception:
            print(
                f"Error reading autoscale formula from {filepath}. Check file path and permissions."
            )
    elif text_input is not None:
        print("Autoscale formula provided via text input.")
        return text_input


def get_sp_secret(config: dict):
    """gets the user's secret from the keyvault based on config

    Args:
        config (dict): contains configuration info

    Returns:
        str: service principal secret

    Example:
        sp_secret = get_sp_secret(config)
    """
    # print("Retrieving service principal secret from Azure Key Vault...")
    try:
        user_credential = DefaultAzureCredential()
        # print("User credential obtained.")
    except Exception as e:
        print("Error obtaining user credentials:", e)

    try:
        secret_client = SecretClient(
            vault_url=config["Authentication"]["vault_url"],
            credential=user_credential,
        )
        # print("Secret client initialized.")
    except KeyError as e:
        print("Error:", e, "Key not found in configuration.")

    try:
        sp_secret = secret_client.get_secret(
            config["Authentication"]["vault_sp_secret_id"]
        ).value
        # print("Service principal secret successfully retrieved.")
        return sp_secret
    except Exception as e:
        print("Error retrieving secret:", e)
        print(
            "Check that vault_uri and vault_sp_secret_id are correctly configured in the config file."
        )


def get_sp_credential(config: dict):
    """gets the user's credentials based on their secret and config file

    Args:
        config (dict): contains configuration info

    Returns:
        class: client credential for Azure Blob Service Client
    """
    # print("Attempting to obtain service principal credentials...")
    sp_secret = get_sp_secret(config)
    try:
        sp_credential = ClientSecretCredential(
            tenant_id=config["Authentication"]["tenant_id"],
            client_id=config["Authentication"]["application_id"],
            client_secret=sp_secret,
        )
        # print("Service principal credentials obtained successfully.")
        return sp_credential
    except KeyError as e:
        print(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Authentication section.",
        )


def get_blob_service_client(config: dict):
    """establishes Blob Service Client using credentials

    Args:
        config (dict): contains configuration info

    Returns:
        class: an instance of BlobServiceClient
    """
    # print("Initializing Blob Service Client...")
    sp_credential = get_sp_credential(config)
    try:
        blob_service_client = BlobServiceClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=sp_credential,
        )
        # print("Blob Service Client successfully created.")
        return blob_service_client
    except KeyError as e:
        print(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Storage section.",
        )


def get_batch_mgmt_client(config: dict):
    """establishes a Batch Management Client based on credentials and config file

    Args:
        config (dict): config dictionary

    Returns:
        class: an instance of the Batch Management Client
    """
    # print("Initializing Batch Management Client...")
    sp_credential = get_sp_credential(config)
    try:
        batch_mgmt_client = BatchManagementClient(
            credential=sp_credential,
            subscription_id=config["Authentication"]["subscription_id"],
        )
        # print("Batch Management Client successfully created.")
        return batch_mgmt_client
    except KeyError as e:
        print(
            f"Configuration error: '{e}' does not exist in the config file. Please add it to the Authentication section.",
        )


def create_blob_containers(
    blob_service_client: BlobServiceClient,
    input_container_name: str = None,
    output_container_name: str = None,
):
    """creates the input and output Blob containers based on given names

    Args:
        blob_service_client (object): an instance of the Batch Management Client
        input_container_name (str): user specified name for input container. Default is None.
        output_container_name (str): user specified name for output container. Default is None.
    """
    # print("Preparing to create blob containers...")
    if input_container_name:
        print(
            f"Attempting to create input container: '{input_container_name}'..."
        )
        create_container(input_container_name, blob_service_client)
    else:
        print(
            "Input container name not specified. Skipping input container creation."
        )

    if output_container_name:
        print(
            f"Attempting to create output container: '{output_container_name}'..."
        )
        create_container(output_container_name, blob_service_client)
    else:
        print(
            "Output container name not specified. Skipping output container creation."
        )


def get_batch_pool_json(
    input_container_name: str,
    output_container_name: str,
    config: dict,
    autoscale_formula_path: str,
):
    """creates a json output with various components needed for batch pool creation

    Args:
        input_container_name (str): user specified name for input container
        output_container_name (str): user specified name for input container
        config (dict): config dictionary
        autoscale_formula_path (str): path to the autoscale formula

    Returns:
        json: relevant information for Batch pool creation
    """
    print("Preparing batch pool configuration...")
    # User-assigned identity for the pool
    user_identity = {
        "type": "UserAssigned",
        "userAssignedIdentities": {
            config["Authentication"]["user_assigned_identity"]: {
                "clientId": config["Authentication"]["client_id"],
                "principalId": config["Authentication"]["principal_id"],
            }
        },
    }
    print("User identity configuration prepared.")

    # Network configuration with no public IP and virtual network
    network_config = {
        "subnetId": config["Authentication"]["subnet_id"],
        "publicIPAddressConfiguration": {"provision": "NoPublicIPAddresses"},
        "dynamicVnetAssignmentScope": "None",
    }
    print("Network configuration prepared.")

    # Virtual machine configuration
    deployment_config = {
        "virtualMachineConfiguration": {
            "imageReference": {
                "publisher": "microsoft-azure-batch",
                "offer": "ubuntu-server-container",
                "sku": "20-04-lts",
                "version": "latest",
            },
            "nodeAgentSkuId": "batch.node.ubuntu 20.04",
            "containerConfiguration": {
                "type": "dockercompatible",
                "containerImageNames": [
                    config["Container"]["container_image_name"]
                ],
                "containerRegistries": [
                    {
                        "registryServer": config["Container"][
                            "container_registry_url"
                        ],
                        "userName": config["Container"][
                            "container_registry_username"
                        ],
                        "password": config["Container"][
                            "container_registry_password"
                        ],
                    }
                ],
            },
        }
    }
    print("VM and container configurations prepared.")

    # Mount configuration
    mount_config = [
        {
            "azureBlobFileSystemConfiguration": {
                "accountName": config["Storage"]["storage_account_name"],
                "identityReference": {
                    "resourceId": config["Authentication"][
                        "user_assigned_identity"
                    ]
                },
                "containerName": input_container_name,
                "blobfuseOptions": "",
                "relativeMountPath": "input",
            }
        },
        {
            "azureBlobFileSystemConfiguration": {
                "accountName": config["Storage"]["storage_account_name"],
                "identityReference": {
                    "resourceId": config["Authentication"][
                        "user_assigned_identity"
                    ]
                },
                "containerName": output_container_name,
                "blobfuseOptions": "",
                "relativeMountPath": "output",
            }
        },
    ]
    print("Mount configuration prepared.")

    # Assemble the pool parameters JSON
    print("Generating autoscale formula...")
    pool_parameters = {
        "identity": user_identity,
        "properties": {
            "vmSize": config["Batch"]["pool_vm_size"],
            "interNodeCommunication": "Disabled",
            "taskSlotsPerNode": 1,
            "taskSchedulingPolicy": {"nodeFillType": "Spread"},
            "deploymentConfiguration": deployment_config,
            "networkConfiguration": network_config,
            "scaleSettings": {
                # "fixedScale": {
                #     "targetDedicatedNodes": 1,
                #     "targetLowPriorityNodes": 0,
                #     "resizeTimeout": "PT15M"
                # }
                "autoScale": {
                    "evaluationInterval": "PT5M",
                    "formula": get_autoscale_formula(
                        filepath=autoscale_formula_path
                    ),
                }
            },
            "resizeOperationStatus": {
                "targetDedicatedNodes": 1,
                "nodeDeallocationOption": "Requeue",
                "resizeTimeout": "PT15M",
                "startTime": "2023-07-05T13:18:25.7572321Z",
            },
            "currentDedicatedNodes": 1,
            "currentLowPriorityNodes": 0,
            "targetNodeCommunicationMode": "Simplified",
            "currentNodeCommunicationMode": "Simplified",
            "mountConfiguration": mount_config,
        },
    }
    print("Batch pool parameters assembled.")

    pool_id = config["Batch"]["pool_id"]
    account_name = config["Batch"]["batch_account_name"]
    resource_group_name = config["Authentication"]["resource_group"]
    batch_json = {
        "user_identity": user_identity,
        "network_confi": network_config,
        "deployment_config": deployment_config,
        "mount_config": mount_config,
        "pool_parameters": pool_parameters,
        "pool_id": pool_id,
        "account_name": account_name,
        "resource_group_name": resource_group_name,
    }
    print("Batch pool JSON configuration is ready.")
    return batch_json


def create_batch_pool(batch_mgmt_client: object, batch_json: dict):
    """creates the Batch pool using the Batch Management Client and info from batch_json

    Args:
        batch_mgmt_client (object): an instance of the Batch Management Client
        batch_json (dict): relevant information for Batch pool creation

    Raises:
        error: pool ID already exists

    Returns:
        str: pool ID value of created pool
    """
    print("Attempting to create the Azure Batch pool...")
    try:
        resource_group_name = batch_json["resource_group_name"]
        account_name = batch_json["account_name"]
        pool_id = batch_json["pool_id"]
        parameters = batch_json["pool_parameters"]

        print(f"Creating pool: {pool_id} in the account: {account_name}...")
        batch_mgmt_client.pool.create(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_id,
            parameters=parameters,
        )
        print(f"Pool {pool_id!r} created successfully.")
    except HttpResponseError as error:
        if "PropertyCannotBeUpdated" in error.message:
            print(f"Pool {pool_id!r} already exists. No further action taken.")
        else:
            print(f"Error creating pool {pool_id!r}: {error}")
            raise
    return pool_id


def delete_pool(resource_group_name: str,
                account_name: str,
                pool_name: str, 
                batch_mgmt_client: object) -> None:
    """deletes the specified pool from Azure Batch.

    Args:
        resource_group_name (str): resource group name
        account_name (str): account name
        pool_name (str): name of pool to delete
        batch_mgmt_client (object): instance of BatchManagementClient
    """
    print(f"Attempting to delete {pool_name}...")
    batch_mgmt_client.pool.begin_delete(
        resource_group_name=resource_group_name,
        account_name=account_name,
        pool_name=pool_name)
    print(f"Pool {pool_name} deleted.")


def list_containers(blob_service_client: object):
    """lists the containers using the BlobServiceClient

    Args:
        blob_service_client (object): an instance of BlobServiceClient

    Returns:
        list[str]: list of containers in Blob Storage account
    """
    # print("Listing all containers in the Blob service account...")
    container_list = []

    for container in blob_service_client.list_containers():
        container_list.append(container.name)
        # print(f"Found container: {container.name}")
    # print("Completed listing containers.")
    return container_list


def upload_files_in_folder(
    folder_name: str,
    input_container_name: str,
    blob_service_client: object,
    verbose: bool = True,
    force_upload: bool = False,
):
    """uploads all files in specified folder to the input container.
    If there are more than 50 files in the folder, the user is asked to confirm
    the upload. This can be bypassed if force_upload = True.

    Args:
        folder_name (str): folder name containing files to be uploaded
        input_container_name (str): the name of the input Blob container
        blob_service_client (object): instance of Blob Service Client
        verbose (bool): whether to print the name of files uploaded. Default True.
        force_upload (bool): whether to force the upload despite the file count in folder. Default False.
    """
    print(f"Checking existence of the container '{input_container_name}'...")
    # check the input container exists
    check = input_container_name in list_containers(blob_service_client)
    if not check:
        print(
            f"Container '{input_container_name}' does not exist in the Blob Storage. Upload aborted."
        )
        return None

    print(
        f"Uploading files from folder '{folder_name}' to container '{input_container_name}'..."
    )
    # Upload the input files
    if not force_upload:
        fnum = []
        for _, _, file in os.walk(os.path.realpath(f"./{folder_name}")):
            fnum.append(len(file))
        fnum_sum = sum(fnum)
        if fnum_sum > 50:
            print(f"You are about to upload {fnum_sum} files.")
            resp = input("Continue? [Y/n]: ")
            if resp == "Y" or resp == "y":
                pass
            else:
                print("Upload aborted.")
                return None
    input_files = []
    for folder, _, file in os.walk(os.path.realpath(f"./{folder_name}")):
        for file_name in file:
            input_files.append(file_name)
            blob_client = blob_service_client.get_blob_client(
                container=input_container_name, blob=file_name
            )
            with open(os.path.join(folder, file_name), "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            if verbose:
                print(f"Uploaded {file_name!r} to {input_container_name}")
    return input_files


def get_batch_service_client(config: dict):
    """creates and returns a Batch Service Client object

    Args:
        config (dict): config dictionary

    Returns:
        object: Batch Service Client object
    """
    # print("Initializing Batch Service Client...")
    sp_secret = get_sp_secret(config)
    batch_client = BatchServiceClient(
        credentials=ServicePrincipalCredentials(
            client_id=config["Authentication"]["application_id"],
            tenant=config["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        ),
        batch_url=config["Batch"]["batch_service_url"],
    )
    # print("Batch Service Client initialized successfully.")
    return batch_client


def add_job(job_id: str, pool_id: str, batch_client: object):
    """takes in a job ID and config to create a job in the pool

    Args:
        job_id (str): name of the job to run
        batch_client (object): batch client object
        config (dict): config dictionary
    """
    print(f"Attempting to create job '{job_id}'...")

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id),
        uses_task_dependencies=True,
    )
    try:
        batch_client.job.add(job)
        print(f"Job '{job_id}' created successfully.")
    except batchmodels.BatchErrorException as err:
        if err.error.code == "JobExists":
            print(f"Job '{job_id}' already exists. No further action taken.")
        else:
            print(f"Error creating job '{job_id}': {err}")
            print("Rename this job or delete the pre-existing job.")
            raise


def add_task_to_job(
    job_id: str,
    task_id_base: str,
    docker_command: str,
    input_files: list[str] | None = None,
    mounts: list | None = None,
    depends_on: str | list[str] | None = None,
    batch_client: object  | None = None,
    full_container_name: str | None = None,
    task_id_max: int = 0,
):
    """add a defined task(s) to a job in the pool

    Args:
        job_id (str): name given to job
        task_id_base (str): the name given to the task_id as a base
        docker_command (str): the docker command to execute for the task
        input_files (list[str]): a  list of input files
        mounts (list[tuple]): a list of tuples in the form (container_name, relative_mount_directory)
        depends_on (str | list[str]): list of tasks this task depends on
        batch_client (object): batch client object
        full_container_name (str): name ACR container to run task on
        task_id_max (int): current max task id in use by Batch

    Returns:
        list: list of task IDs created
    """
    # convert docker command to string if in list format
    if isinstance(docker_command, list):
        d_cmd_str = " ".join(docker_command)
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
        task_deps = batchmodels.TaskDependencies(task_ids=depends_on)

    mount_str = ""
    # src = env variable to fsmounts/rel_path
    # target = the directory(path) you reference in your code
    if mounts is not None:
        mount_str = ""
        for mount in mounts:
            mount_str = mount_str +  "--mount type=bind,source=" + az_mount_dir + f"/{mount[1]},target=/{mount[1]} "


    if input_files:
        tasks = []
        for i, input_file in enumerate(input_files):
            config_stem = "_".join(input_file.split(".")[:-1]).split("/")[-1]
            id = task_id_base + "-" + config_stem
            # shorten the id name to fit the 64 char limit of task ids
            if len(id) > 64:
                id = id[:60] + "_" + str(i)
            tasks.append(id)
            task = batchmodels.TaskAddParameter(
                id=id,
                command_line=d_cmd_str + " " + input_mount_dir + input_file,
                container_settings=batchmodels.TaskContainerSettings(
                    image_name=full_container_name,
                    container_run_options=f"--name={job_id} --rm " + mount_str,
                ),
                user_identity=user_identity,
                depends_on=task_deps,
            )
            batch_client.task.add(job_id=job_id, task=task)
            print(f"Task '{id}' added to job '{job_id}'.")
        return tasks
    else:
        task_id = f"{task_id_base}-{str(task_id_max + 1)}"
        command_line = d_cmd_str
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
        )
        batch_client.task.add(job_id=job_id, task=task)
        print(
            f"Task '{task_id}' added to job '{job_id}'."
        )
        t = []
        t.append(task_id)
        return t


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
    #start monitoring
    print(
        f"Starting to monitor tasks for job '{job_id}' with a timeout of {timeout} minutes."
    )
    start_time = datetime.datetime.now().replace(microsecond=0)
    _timeout = datetime.timedelta(minutes=timeout)
    timeout_expiration = start_time + _timeout

    print(
        f"Job '{job_id}' monitoring started at {start_time}. Timeout at {timeout_expiration}."
    )
    print("-" * 20)

    # count tasks and print to user the starting value
    # as tasks complete, print which complete
    # print remaining number of tasks
    tasks = list(batch_client.task.list(job_id))

    #get total tasks
    total_tasks = len([task for task in tasks])
    print(f"Total tasks to monitor: {total_tasks}")

    #pool setup and status
    

    completed = False
    while datetime.datetime.now() < timeout_expiration:
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

        for task in completed_tasks:
            if task.as_dict()["execution_info"]["result"] == "failure":
                failures += 1
            elif task.as_dict()["execution_info"]["result"] == "success":
                successes += 1

        print("completed:", completions)
        print("remaining:", incompletions)
        print("successes:", successes)
        print("failures:", failures)

        if not incomplete_tasks:
            print("\nAll tasks completed.")
            completed = True
            break

    if completed:
        print(
            "All tasks have reached 'Completed' state within the timeout period."
        )
    else:
        raise RuntimeError(
            f"ERROR: Tasks did not reach 'Completed' state within timeout period of {timeout} minutes."
        )

    end_time = datetime.datetime.now().replace(microsecond=0)
    runtime = end_time - start_time
    print(f"Monitoring ended: {end_time}. Total elapsed time: {runtime}.")
    return {"completed": completed, "elapsed time": runtime}



def list_files_in_container(
    container_name: str, sp_credential: str, config: dict
):
    """lists out files in blob container

    Args:
        container_name (str): the name of the container to get files
        sp_credential (str): the service principal credential
        config (dict): configuration dictionary

    Returns:
        list: list of file names in the container
    """
    print(f"Listing files in container '{container_name}'...")
    try:
        cc = ContainerClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=sp_credential,
            container_name=container_name,
        )
        files = [f for f in cc.list_blob_names()]
        print(f"Found {len(files)} files in container '{container_name}'.")
        return files
    except Exception as e:
        print(f"Error connecting to container '{container_name}': {e}")
        return None

    files = []
    for f in cc.list_blob_names():
        files.append(
            f
        )  # gather a list of file names from list_blob_names iterator
    return files


def df_to_yaml(df: pd.DataFrame):
    """converts a pandas dataframe to yaml

    Args:
        df (pd.DataFrame): pandas dataframe to convert to yaml

    Returns:
        dict:  yaml string converted from dataframe
    """
    print("Converting DataFrame to YAML format...")
    yaml_str = dump(
        df.to_dict(orient="records"), sort_keys=False, default_flow_style=False
    )
    print("Conversion complete.")
    return yaml_str


def yaml_to_df(yaml_file: dict):
    """converts a yaml file to pandas dataframe

    Args:
        yaml_file (dict): yaml file

    Returns:
        pd.DataFrame: pandas dataframe converted from yaml file
    """
    print("Converting YAML to DataFrame...")
    df = pd.json_normalize(load(yaml_file, Loader=SafeLoader))
    print("Conversion complete.")
    return df


def edit_yaml_r0(file_path: str, r0_start=1, r0_end=4, step=0.1):
    """takes in a yaml file and produces replicate yaml files with the r0 changed based on the start, stop, and step provided. Output yamls go to yaml/ folder.

    Args:
        file_path (str): path to file
        r0_start (int, optional): The lower end of the r0 range. Defaults to 1.
        r0_end (int, optional): The upped end of the r0 range (inclusive). Defaults to 4.
        step (float, optional): The step size of each r0 increase. Defaults to 0.1.
    """
    print(
        f"Starting to edit YAML file '{file_path}' with r0 range from {r0_start} to {r0_end} by steps of {step}."
    )

    with open(file_path, "r") as file:
        y = yaml.safe_load(file)

    r0_list = np.arange(r0_start, r0_end + step, step, dtype=float).tolist()
    for r0 in r0_list:
        r0 = round(r0, len(str(step).split(".")[1]))
        y["baseScenario"]["r0"] = r0
        y["outputDirectory"] = os.path.join(y["outputDirectory"], str(r0))
        outfile = f"{file_path.replace('.yaml', '')}_{str(r0).replace('.', '-')}.yaml"
        with open(outfile, "w") as f:
            yaml.dump(y, f, default_flow_style=False)
        print(f"Generated modified YAML file with r0={r0} at '{outfile}'.")
    print("Completed editing YAML files.")


def get_user_identity(config: str):
    """gets the user identity based on the config information.

    Args:
        config (str): config dict

    Returns:
        dict: the dictionary containing user identity information to be used with the pool parameters.
    """
    user_identity = {
        "type": "UserAssigned",
        "userAssignedIdentities": {
            config["Authentication"]["user_assigned_identity"]: {
                "clientId": config["Authentication"]["client_id"],
                "principalId": config["Authentication"]["principal_id"],
            }
        },
    }
    return user_identity


def get_network_config(config: str):
    """gets the network configuration based on the config information

    Args:
        config (str): config dict

    Returns:
        dict: the dictionary containing network configurations to be used with the pool parameters.
    """
    network_config = {
        "subnetId": config["Authentication"]["subnet_id"],
        "publicIPAddressConfiguration": {"provision": "NoPublicIPAddresses"},
        "dynamicVnetAssignmentScope": "None",
    }
    return network_config


def get_deployment_config(
    container_image_name: str,
    container_registry_url: str,
    container_registry_server: str,
    config: str,
):
    """gets the deployment config based on the config information

    Args:
        container_image_name (str): container image name
        container_registry_url (str): container registry URL
        container_registry_server (str): container registry server
        config (str): config dict

    Returns:
        dict: dictionary containing info for container deployment. Uses ubuntu server with info obtained from config file.
    """
    deployment_config = {
        "virtualMachineConfiguration": {
            "imageReference": {
                "publisher": "microsoft-azure-batch",
                "offer": "ubuntu-server-container",
                "sku": "20-04-lts",
                "version": "latest",
            },
            "nodeAgentSkuId": "batch.node.ubuntu 20.04",
            "containerConfiguration": {
                "type": "dockercompatible",
                "containerImageNames": [container_image_name],
                "containerRegistries": [
                    {
                        "registryUrl": container_registry_url,
                        "userName": config["Container"][
                            "container_registry_username"
                        ],
                        "password": config["Container"][
                            "container_registry_password"
                        ],
                        "registryServer": container_registry_server,
                    }
                ],
            },
        }
    }
    return deployment_config


def get_blob_config(container_name: str, rel_mount_path: str, cache_blobfuse: bool, config: dict):
    """gets the blob storage configuration based on the config information

    Args:
        container_name (str): name of Blob Storage Container
        rel_mount_path (str): relative mount path
        cache_blobfuse (bool): True to use blobfuse caching, False to download data from blobfuse every time
        config (dict): config dict

    Returns:
        dict: dictionary containing info for blob storage configuration. Used as input to get_mount_config().
    """
    print(
        f"Generating blob configuration for container '{container_name}' with mount path '{rel_mount_path}'..."
    )
    if cache_blobfuse:
        blob_str = ""
    else:
        blob_str = "-o direct_io"
    
    blob_config = {
        "azureBlobFileSystemConfiguration": {
            "accountName": config["Storage"]["storage_account_name"],
            "identityReference": {
                "resourceId": config["Authentication"][
                    "user_assigned_identity"
                ]
            },
            "containerName": container_name,
            "blobfuseOptions": blob_str,
            "relativeMountPath": rel_mount_path,
        }
    }
    return blob_config


def get_mount_config(blob_config: list[str]):
    """takes blob configurations as input and combines them to create a mount configuration.

    Args:
        Blob configurations, usually from get_blob_config(). Usually one for input blob and one for output blob.

    Returns:
        list: mount configuration to used with get_pool_parameters.
    """
    _mount_config = []
    for blob in blob_config:
            _mount_config.append(blob)
    return _mount_config

def get_pool_parameters(
    mode: str,
    container_image_name: str,
    container_registry_url: str,
    container_registry_server: str,
    config: dict,
    mount_config: list,
    autoscale_formula_path: str = None,
    timeout: int = 60,
    dedicated_nodes: int = 1,
    low_priority_nodes: int = 0,
    use_default_autoscale_formula: bool = False,
    max_autoscale_nodes: int = 3
):
    """creates a pool parameter dictionary to be used with pool creation.

    Args:
        mode (str): either 'fixed' or 'autoscale'
        container_image_name (str): container image name
        container_registry_url (str): container registry URL
        container_registry_server (str): container registry server
        config (dict): config dict
        mount_config (list): output from get_mount_config() regarding mounting of blob storage
        autoscale_formula_path (str, optional): path to autoscale formula file if mode is 'autoscale'. Defaults to None.
        timeout (int, optional): length in minutes of timeout for tasks that run in this pool. Defaults to 60.
        dedicated_nodes (int, optional): number of dedicated nodes. Defaults to 1.
        low_priority_nodes (int, optional): number of low priority nodes. Defaults to 0.
        use_default_autoscale_formula (bool, optional)

    Returns:
        dict: dict of pool parameters for pool creation
    """
    print(
        f"Setting up pool parameters in '{mode}' mode with timeout={timeout} minutes..."
    )
    if mode == "fixed":
        scale_settings = {
            "fixedScale": {
                "targetDedicatedNodes": dedicated_nodes,
                "targetLowPriorityNodes": low_priority_nodes,
                "resizeTimeout": f"PT{timeout}M",
            }
        }
    elif mode == "autoscale" and use_default_autoscale_formula is False:
        scale_settings = {
            "autoScale": {
                "evaluationInterval": "PT5M",
                "formula": get_autoscale_formula(
                    filepath=autoscale_formula_path
                ),
            }
        }
    elif mode == "autoscale" and use_default_autoscale_formula is True:
        scale_settings = {
            "autoScale": {
                "evaluationInterval": "PT5M",
                "formula": generate_autoscale_formula(
                    max_nodes= max_autoscale_nodes
                ),
            }
        }
    else:
        return {}

    pool_parameters = {
        "identity": get_user_identity(config),
        "properties": {
            "vmSize": config["Batch"]["pool_vm_size"],
            "interNodeCommunication": "Disabled",
            "taskSlotsPerNode": 1,
            "taskSchedulingPolicy": {"nodeFillType": "Spread"},
            "deploymentConfiguration": get_deployment_config(
                container_image_name,
                container_registry_url,
                container_registry_server,
                config,
            ),
            "networkConfiguration": get_network_config(config),
            "scaleSettings": scale_settings,
            "resizeOperationStatus": {
                "targetDedicatedNodes": 1,
                "nodeDeallocationOption": "Requeue",
                "resizeTimeout": "PT15M",
                "startTime": "2023-07-05T13:18:25.7572321Z",
            },
            "currentDedicatedNodes": 1,
            "currentLowPriorityNodes": 0,
            "targetNodeCommunicationMode": "Simplified",
            "currentNodeCommunicationMode": "Simplified",
            "mountConfiguration": mount_config,
        },
    }
    print("Pool parameters successfully configured.")
    return pool_parameters


def check_blob_existence(c_client: ContainerClient, blob_name: str) -> bool:
    """Checks whether a blob exists in the specified container

    Args:
        c_client (ContainerClient): an Azure Container Client object
        blob_name (str): name of Blob to check for existence

    Returns:
        bool: whether the specified Blob exists

    """
    blob = c_client.get_blob_client(blob=blob_name)
    return blob.exists()


def check_virtual_directory_existence(
    c_client: ContainerClient, vdir_path: str
) -> bool:
    """Checks whether any blobs exist with the specified virtual directory path

    Args:
        c_client (ContainerClient): an Azure Container Client object
        vdir_path (str): path of virtual directory

    Returns:
        bool: whether the virtual directory exists

    """
    blobs = c_client.list_blobs(name_starts_with=vdir_path)
    try:
        first_blob = next(blobs)
        print(f"{first_blob.name} found.")
        return True
    except StopIteration as e:
        print(repr(e))
        return False


def download_file(
    c_client: ContainerClient,
    src_path: str,
    dest_path: str,
    do_check: bool = True,
) -> None:
    """
    Download a file from Azure Blob storage

    Args:
        client (ContainerClient):
            Instance of ContainerClient provided with the storage account
        src_path (str):
            Path within the container to the desired file (including filename)
        dest_path (str):
            Path to desired location to save the downloaded file
        container (str):
            Name of the storage container containing the file to be downloaded
        do_check (bool):
            Whether or not to do an existence check

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    if do_check and not check_blob_existence(c_client, src_path):
        raise ValueError(f"Source blob: {src_path} does not exist.")
    dest_path = Path(dest_path)
    dest_path.parents[0].mkdir(parents=True, exist_ok=True)
    with dest_path.open(mode="wb") as blob_download:
        download_stream = c_client.download_blob(blob=src_path)
        blob_download.write(download_stream.readall())


def download_directory(
    c_client: ContainerClient, src_path: str, dest_path: str
) -> None:
    """
    Downloads a directory using prefix matching and the .list_blobs() method

    Args:
        client (ContainerClient):
            Instance of ContainerClient provided with the storage account
            and container
        src_path (str):
            Prefix of the blobs to download
        dest_path (str):
            Path to the directory in which to store the downloads

    Raises:
        ValueError:
            When no blobs exist with the specified prefix (src_path)
    """
    if not check_virtual_directory_existence(c_client, src_path):
        raise ValueError(
            f"Source virtual directory: {src_path} does not exist."
        )
    for blob in c_client.list_blobs(name_starts_with=src_path):
        download_file(
            c_client, blob.name, os.path.join(dest_path, blob.name), False
        )


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
        return True
    else:
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

    Returns:
        str: full container name
    """
    # check if Dockerfile exists
    try:
        d = docker.from_env(timeout=10).ping()
    except DockerException:
        print("Could not ping Docker. Make sure Docker is running.")
        print("Container not packaged/uploaded.")
        print("Try again when Docker is running.")
        return None

    if os.path.exists(path_to_dockerfile) and d:
        full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"
        print(f"full container name: {full_container_name}")
        # Build container
        sp.run(
            f"docker image build -f {path_to_dockerfile} -t {full_container_name} .",
            shell=True,
        )
        # Upload container to registry
        # upload with device login if desired
        if use_device_code:
            sp.run("az login --use-device-code", shell=True)
        else:
            sp.run("az login", shell=True)
        sp.run(f"az acr login --name {registry_name}", shell=True)
        sp.run(f"docker push {full_container_name}", shell=True)
        return full_container_name
    else:
        print("Dockerfile does not exist in the root of the directory.")


def check_pool_exists(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
):
    """Check if a pool exists in Azure Batch

    Args:
        resource_group_name (str):
        account_name (str):
        pool_name (str):
        batch_mgmt_client (object):

    Returns:
        bool: whether the pool exists
    """
    try:
        batch_mgmt_client.pool.get(
            resource_group_name, account_name, pool_name
        )
        return True
    except Exception:
        return False


def get_pool_info(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> dict:
    """Get the basic information for a specified pool.

    Args:
        resource_group_name (str): name of resource group
        account_name (str): name of account
        pool_name (str): name of pool
        batch_mgmt_client (object): instance of Batch Management Client

    Returns:
        dict: json with name, last_modified, creation_time, vm_size, and task_slots_per_node info
    """
    result = batch_mgmt_client.pool.get(
        resource_group_name, account_name, pool_name
    )
    j = {
        "name": result.name,
        "last_modified": result.last_modified.strftime("%m/%d/%y %H:%M"),
        "creation_time": result.creation_time.strftime("%m/%d/%y %H:%M"),
        "vm_size": result.vm_size,
        "task_slots_per_node": result.task_slots_per_node,
    }
    return json.dumps(j)


def get_pool_full_info(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> dict:
    """Get the full information of a specified pool.

    Args:
        resource_group_name (str): name of resource group
        account_name (str): name of account
        pool_name (str): name of pool
        batch_mgmt_client (object): instance of Batch Management Client

    Returns:
        dict: dictionary with full pool information
    """
    result = batch_mgmt_client.pool.get(
        resource_group_name, account_name, pool_name
    )
    return result


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
            "Authentication.client_id",
            "Authentication.principal_id",
            "Authentication.application_id",
            "Authentication.vault_url",
            "Authentication.vault_sp_secret_id",
            "Authentication.vault_sa_secret_id",
            "Authentication.vault_ab_secret_id",
            "Authentication.subnet_id",
            "Batch.batch_account_name",
            "Batch.batch_url",
            "Batch.batch_service_url",
            "Batch.pool-node-count",
            "Batch.pool_vm_size",
            "Storage.storage_account_name",
            "Storage.storage_account_url",
            "Container.container_registry_username",
            "Container.container_registry_password",
        ]
    )
    loaded = set(pd.json_normalize(config).columns)
    check = req - loaded == set()
    if check:
        return True
    else:
        print(
            str(list(req - loaded)),
            "missing from the config file and will be required by client.",
        )
        return False


def check_azure_container_exists(
    registry_name: str, repo_name: str, tag_name: str
) -> str:
    """specify the container in ACR to use without packaging and uploading the docker container from local.

    Args:
        registry_name (str): the name of the registry in Azure Container Registry
        repo_name (str): the name of the repo
        tag_name (str): the tag name

    Returns:
        str: full name of container
    """
    # check full_container_name exists in ACR
    audience = "https://management.azure.com"
    endpoint = f"https://{registry_name}.azurecr.io"
    try:
        # check full_container_name exists in ACR
        cr_client = ContainerRegistryClient(
            endpoint, DefaultAzureCredential(), audience=audience
        )
    except Exception as e:
        print(
            f"Registry [{registry_name}] or repo [{repo_name}] does not exist"
        )
        print(e)
        raise Exception
    tag_list = []
    for tag in cr_client.list_tag_properties(repo_name):
        tag_properties = cr_client.get_tag_properties(repo_name, tag.name)
        tag_list.append(tag_properties.name)
    print("Available tags in repo:", tag_list)
    if tag_name in tag_list:
        print(f"setting {registry_name}/{repo_name}:{tag_name}")
        full_container_name = (
            f"{registry_name}.azurecr.io/{repo_name}:{tag_name}"
        )
        return full_container_name
    else:
        print(f"{registry_name}/{repo_name}:{tag_name} does not exist")
        return None

def generate_autoscale_formula(max_nodes: int = 8) -> str:
    """
    Generate a generic autoscale formula for use based on maximum number of nodes to scale up to.

    Args:
        max_nodes (int): maximum number of nodes to cap the pool at

    Returns:
        str: the text of an autoscale formula

    """
    formula  = f"""
    // Get pending tasks for the past 10 minutes.
    $samples = $ActiveTasks.GetSamplePercent(TimeInterval_Minute * 10);
    // If we have fewer than 70 percent data points, we use the last sample point, otherwise we use the maximum of last sample point and the history average.
    $tasks = $samples < 70 ? max(0, $ActiveTasks.GetSample(1)) :
    max( $ActiveTasks.GetSample(1), avg($ActiveTasks.GetSample(TimeInterval_Minute * 10)));
    // If number of pending tasks is not 0, set targetVM to pending tasks, otherwise half of current dedicated.
    $targetVMs = $tasks > 0 ? $tasks : max(0, $TargetDedicatedNodes / 2);
    // The pool size is capped to max_nodes input
    cappedPoolSize = {max_nodes};
    $TargetDedicatedNodes = max(0, min($targetVMs, cappedPoolSize));
    // Set node deallocation mode - keep nodes active only until tasks finish
    $NodeDeallocationOption = taskcompletion;
    """
    return formula

def format_rel_path(rel_path: str) -> str:
    if rel_path.startswith("/"):
        rel_path = rel_path[1:]
    return rel_path