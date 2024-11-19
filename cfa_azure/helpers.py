# import modules for use
import datetime
import json
import logging
import os
import subprocess as sp
import time
from datetime import datetime as dt
from os import path, walk
from pathlib import Path
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
    OnTaskFailure,
)
from azure.common.credentials import ServicePrincipalCredentials
from azure.containerregistry import ContainerRegistryClient
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.batch import BatchManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from docker.errors import DockerException
from yaml import SafeLoader, dump, load

logger = logging.getLogger(__name__)


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
        # get default autoscale formula:
        autoscale_text = generate_autoscale_formula()
        logger.debug(
            "Default autoscale formula used. Please provide a path to autoscale formula to sepcify your own formula."
        )
        return autoscale_text
    elif filepath is not None:
        try:
            with open(filepath, "r") as autoscale_text:
                logger.debug(
                    f"Autoscale formula successfully read from {filepath}."
                )
                return autoscale_text.read()
        except Exception:
            logger.error(
                f"Error reading autoscale formula from {filepath}. Check file path and permissions."
            )
            raise Exception(
                f"Error reading autoscale formula from {filepath}. Check file path and permissions."
            ) from None
    elif text_input is not None:
        logger.debug("Autoscale formula provided via text input.")
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
    logger.debug("Attempting to retrieve Azure credential.")
    try:
        user_credential = DefaultAzureCredential()
        logger.debug("Credential obtained.")
    except Exception as e:
        logger.error("Error obtaining credential:", e)
        raise e

    logger.debug("Attempting to establish secret client.")
    try:
        secret_client = SecretClient(
            vault_url=config["Authentication"]["vault_url"],
            credential=user_credential,
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


def get_sp_credential(config: dict):
    """gets the user's credentials based on their secret and config file

    Args:
        config (dict): contains configuration info

    Returns:
        class: client credential for Azure Blob Service Client
    """
    logger.debug("Attempting to obtain service principal credentials...")
    sp_secret = get_sp_secret(config)
    try:
        sp_credential = ClientSecretCredential(
            tenant_id=config["Authentication"]["tenant_id"],
            client_id=config["Authentication"]["sp_application_id"],
            client_secret=sp_secret,
        )
        logger.debug("Service principal credentials obtained successfully.")
        return sp_credential
    except KeyError as e:
        logger.error(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Authentication section.",
        )
        raise e


def get_blob_service_client(config: dict):
    """establishes Blob Service Client using credentials

    Args:
        config (dict): contains configuration info

    Returns:
        class: an instance of BlobServiceClient
    """
    logger.debug("Initializing Blob Service Client...")
    sp_credential = get_sp_credential(config)
    try:
        blob_service_client = BlobServiceClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=sp_credential,
        )
        logger.debug("Blob Service Client successfully created.")
        return blob_service_client
    except KeyError as e:
        logger.error(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Storage section.",
        )
        raise e


def get_batch_mgmt_client(config: dict):
    """establishes a Batch Management Client based on credentials and config file

    Args:
        config (dict): config dictionary

    Returns:
        class: an instance of the Batch Management Client
    """
    logger.debug("Initializing Batch Management Client...")
    sp_credential = get_sp_credential(config)
    try:
        batch_mgmt_client = BatchManagementClient(
            credential=sp_credential,
            subscription_id=config["Authentication"]["subscription_id"],
        )
        logger.debug("Batch Management Client successfully created.")
        return batch_mgmt_client
    except KeyError as e:
        logger.error(
            f"Configuration error: '{e}' does not exist in the config file. Please add it to the Authentication section.",
        )
        raise e


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
        logger.info(
            f"Attempting to create input container: '{input_container_name}'..."
        )
        create_container(input_container_name, blob_service_client)
    else:
        logger.warning(
            "Input container name not specified. Skipping input container creation."
        )

    if output_container_name:
        logger.info(
            f"Attempting to create output container: '{output_container_name}'..."
        )
        create_container(output_container_name, blob_service_client)
    else:
        logger.warning(
            "Output container name not specified. Skipping output container creation."
        )


def get_batch_pool_json(
    input_container_name: str,
    output_container_name: str,
    config: dict,
    autoscale_formula_path: str = None,
    autoscale_evaluation_interval: str = "PT5M",
    fixedscale_resize_timeout: str = "PT15M",
    container_image_name: str = None,
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
    logger.debug("Preparing batch pool configuration...")
    # User-assigned identity for the pool
    user_identity = {
        "type": "UserAssigned",
        "userAssignedIdentities": {
            config["Authentication"]["user_assigned_identity"]: {
                "clientId": config["Authentication"]["batch_application_id"],
                "principalId": config["Authentication"]["batch_object_id"],
            }
        },
    }
    logger.debug("User identity configuration prepared.")

    # Network configuration with no public IP and virtual network
    network_config = {
        "subnetId": config["Authentication"]["subnet_id"],
        "publicIPAddressConfiguration": {"provision": "NoPublicIPAddresses"},
        "dynamicVnetAssignmentScope": "None",
    }
    logger.debug("Network configuration prepared.")

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
        }
    }
    if container_image_name:
        container_configuration = {
            "type": "dockercompatible",
            "containerImageNames": [container_image_name],
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
        }
        deployment_config["virtualMachineConfiguration"][
            "containerConfiguration"
        ] = container_configuration
    logger.debug("VM and container configurations prepared.")

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
    logger.debug("Mount configuration prepared.")

    # Assemble the pool parameters JSON
    logger.debug("Generating autoscale formula...")
    pool_parameters = {
        "identity": user_identity,
        "properties": {
            "vmSize": config["Batch"]["pool_vm_size"],
            "interNodeCommunication": "Disabled",
            "taskSlotsPerNode": 1,
            "taskSchedulingPolicy": {"nodeFillType": "Spread"},
            "deploymentConfiguration": deployment_config,
            "networkConfiguration": network_config,
            "resizeOperationStatus": {
                "targetDedicatedNodes": 1,
                "nodeDeallocationOption": "Requeue",
                "resizeTimeout": fixedscale_resize_timeout,
                "startTime": "2023-07-05T13:18:25.7572321Z",
            },
            "currentDedicatedNodes": 1,
            "currentLowPriorityNodes": 0,
            "targetNodeCommunicationMode": "Simplified",
            "currentNodeCommunicationMode": "Simplified",
            "mountConfiguration": mount_config,
        },
    }
    if autoscale_formula_path:
        pool_parameters["properties"]["scaleSettings"] = {
            # "fixedScale": {
            #     "targetDedicatedNodes": 1,
            #     "targetLowPriorityNodes": 0,
            #     "resizeTimeout": "PT15M"
            # }
            "autoScale": {
                "evaluationInterval": autoscale_evaluation_interval,
                "formula": get_autoscale_formula(
                    filepath=autoscale_formula_path
                ),
            }
        }

    logger.debug("Batch pool parameters assembled.")

    pool_id = config["Batch"]["pool_id"]
    account_name = config["Batch"]["batch_account_name"]
    resource_group_name = config["Authentication"]["resource_group"]
    batch_json = {
        "user_identity": user_identity,
        "network_config": network_config,
        "deployment_config": deployment_config,
        "mount_config": mount_config,
        "pool_parameters": pool_parameters,
        "pool_id": pool_id,
        "account_name": account_name,
        "resource_group_name": resource_group_name,
    }
    logger.debug("Batch pool JSON configuration is ready.")
    return batch_json


def update_pool(
    pool_name: str,
    pool_parameters: dict,
    batch_mgmt_client: object,
    account_name: str,
    resource_group_name: str,
) -> dict:
    print("Updating the pool...")

    start_time = datetime.datetime.now()
    print(f"Updating the pool '{pool_name}'...")
    batch_mgmt_client.pool.update(
        resource_group_name=resource_group_name,
        account_name=account_name,
        pool_name=pool_name,
        parameters=pool_parameters,
    )

    end_time = datetime.datetime.now()
    updation_time = round((end_time - start_time).total_seconds(), 2)
    print(f"Pool update process completed in {updation_time} seconds.")

    return {
        "pool_id": pool_name,
        "updation_time": updation_time,
    }


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
    logger.info("Attempting to create the Azure Batch pool...")
    try:
        resource_group_name = batch_json["resource_group_name"]
        account_name = batch_json["account_name"]
        pool_id = batch_json["pool_id"]
        parameters = batch_json["pool_parameters"]

        logger.info(
            f"Creating pool: {pool_id} in the account: {account_name}..."
        )
        new_pool = batch_mgmt_client.pool.create(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_id,
            parameters=parameters,
        )
        pool_id = new_pool.name
        logger.info(f"Pool {pool_id!r} created successfully.")
    except HttpResponseError as error:
        if "PropertyCannotBeUpdated" in error.message:
            logger.error(
                f"Pool {pool_id!r} already exists. No further action taken."
            )
            raise error
        else:
            logger.error(f"Error creating pool {pool_id!r}: {error}")
            raise
    return pool_id


def delete_pool(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> None:
    """deletes the specified pool from Azure Batch.

    Args:
        resource_group_name (str): resource group name
        account_name (str): account name
        pool_name (str): name of pool to delete
        batch_mgmt_client (object): instance of BatchManagementClient
    """
    logger.debug(f"Attempting to delete {pool_name}...")
    poller = batch_mgmt_client.pool.begin_delete(
        resource_group_name=resource_group_name,
        account_name=account_name,
        pool_name=pool_name,
    )
    logger.info(f"Pool {pool_name} deleted.")
    return poller


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
        logger.debug(f"Found container: {container.name}")
    logger.debug("Completed listing containers.")
    return container_list


def upload_blob_file(
    filepath: str,
    location: str = "",
    container_client: object = None,
    verbose: bool = False,
):
    """Uploads a specified file to Blob storage.
    Args:
        filepath (str): the path to the file.
        location (str): the location (folder) inside the Blob container. Uploaded to root if "". Default is "".
        container_client: a ContainerClient object to interact with Blob container.
        verbose (bool): whether to be verbose in uploaded files. Defaults to False

    Example:
        upload_blob_file("sample_file.txt", container_client = cc, verbose = False)
        - uploads the "sample_file.txt" file to the root of the blob container

        upload_blob_file("sample_file.txt", "job_1/input", cc, False)
        - uploads the "sample_file.txt" file to the job_1/input folder of the blob container.
        - note that job_1/input will be created if it does not exist.
    """
    if location.startswith("/"):
        location = location[1:]
    with open(file=filepath, mode="rb") as data:
        _, _file = path.split(filepath)
        _name = path.join(location, _file)
        container_client.upload_blob(name=_name, data=data, overwrite=True)
    if verbose:
        print(
            f"Uploaded {filepath} to {container_client.container_name} as {_name}."
        )
        logger.info(
            f"Uploaded {filepath} to {container_client.container_name} as {_name}."
        )


def walk_folder(folder: str) -> list | None:
    file_list = []
    for dirname, _, fname in walk(folder):
        for f in fname:
            _path = path.join(dirname, f)
            file_list.append(_path)
    return file_list


def upload_files_in_folder(
    folder: str,
    container_name: str,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    location_in_blob: str = "",
    blob_service_client=None,
    verbose: bool = True,
    force_upload: bool = True,
):
    """uploads all files in specified folder to the input container.
    If there are more than 50 files in the folder, the user is asked to confirm
    the upload. This can be bypassed if force_upload = True.

    Args:
        folder (str): folder name containing files to be uploaded
        container_name (str): the name of the Blob container
        include_extensions (str, list): a string or list of extensions desired for upload. Optional. Example: ".py" or [".py", ".csv"]
        exclude_extensions (str, list): a string or list of extensions of files not to include in the upload. Optional. Example: ".py" or [".py", ".csv"]
        location_in_blob (str): location (folder) to upload in Blob container. Will create the folder if it does not exist. Default is "" (root of Blob Container).
        blob_service_client (object): instance of Blob Service Client
        verbose (bool): whether to print the name of files uploaded. Default True.
        force_upload (bool): whether to force the upload despite the file count in folder. Default False.

    Returns:
        list: list of files uploaded
    """
    # check that include and exclude extensions are not both used, format if exist
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    elif exclude_extensions is not None:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error(
            "Use included_extensions or exclude_extensions, not both."
        )
        raise Exception(
            "Use included_extensions or exclude_extensions, not both."
        ) from None
    # check container exists
    logger.debug(f"Checking Blob container {container_name} exists.")
    # create container client
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    # check if container client exists
    if not container_client.exists():
        logger.error(
            f"Blob container {container_name} does not exist. Please try again with an existing Blob container."
        )
        raise Exception(
            f"Blob container {container_name} does not exist. Please try again with an existing Blob container."
        ) from None
    # check number of files if force_upload False
    logger.debug(f"Blob container {container_name} found. Uploading files...")
    # check if files should be force uploaded
    if not force_upload:
        fnum = []
        for _, _, file in os.walk(os.path.realpath(f"./{folder}")):
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
    # get all files in folder
    file_list = []
    # check if folder is valid
    if not path.isdir(folder):
        logger.warning(
            f"{folder} is not a folder/directory. Make sure to specify a valid folder."
        )
        return None
    file_list = walk_folder(folder)
    # create sublist matching include_extensions and exclude_extensions
    flist = []
    if include_extensions is None:
        if exclude_extensions is not None:
            # find files that don't contain the specified extensions
            for _file in file_list:
                if os.path.splitext(_file)[1] not in exclude_extensions:
                    flist.append(_file)
        else:  # this is for no specified extensions to include of exclude
            flist = file_list
    else:
        # include only specified extension files
        for _file in file_list:
            if os.path.splitext(_file)[1] in include_extensions:
                flist.append(_file)

    # iteratively call the upload_blob_file function to upload individual files
    for file in flist:
        # get the right folder location, need to drop the folder from the beginning and remove the file name, keeping only middle folders
        drop_folder = path.dirname(file).replace(folder, "", 1)
        if drop_folder.startswith("/"):
            drop_folder = drop_folder[
                1:
            ]  # removes the / so path.join doesnt mistake for root
        logger.debug(f"Calling upload_blob_file for {file}")
        upload_blob_file(
            file,
            path.join(location_in_blob, drop_folder),
            container_client,
            verbose,
        )
    return file_list


def get_batch_service_client(config: dict):
    """creates and returns a Batch Service Client object

    Args:
        config (dict): config dictionary

    Returns:
        object: Batch Service Client object
    """
    logger.debug("Initializing Batch Service Client...")
    logger.debug("Pulling in SP Secret for batch client.")
    sp_secret = get_sp_secret(config)
    logger.debug("Attempting to create Batch Service Client.")
    batch_client = BatchServiceClient(
        credentials=ServicePrincipalCredentials(
            client_id=config["Authentication"]["sp_application_id"],
            tenant=config["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        ),
        batch_url=config["Batch"]["batch_service_url"],
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
    job_id: str, pool_id: str, batch_client: object, task_retries: int = 0
):
    """takes in a job ID and config to create a job in the pool

    Args:
        job_id (str): name of the job to run
        pool_id (str): name of pool
        batch_client (object): batch client object
        task_retries (int): number of times to retry a failing task. Default 0.
    """
    logger.debug(f"Attempting to create job '{job_id}'...")
    logger.debug("Adding job parameters to job.")
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id),
        uses_task_dependencies=True,
        on_task_failure=OnTaskFailure.perform_exit_options_job_action,
        constraints=JobConstraints(max_task_retry_count=task_retries),
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
    input_files: list[str] | None = None,
    mounts: list | None = None,
    depends_on: str | list[str] | None = None,
    run_dependent_tasks_on_fail: bool = False,
    batch_client: object | None = None,
    full_container_name: str | None = None,
    task_id_max: int = 0,
):
    """add a defined task(s) to a job in the pool

    Args:
        job_id (str): name given to job
        task_id_base (str): the name given to the task_id as a base
        docker_command (str): the docker command to execute for the task
        save_logs_rel_path (str): relative path to blob where logs should be stored. Default None for not saving logs.
        logs_folder (str): folder structure to save stdout logs to in blob container. Default is stdout_stderr.
        name_suffix (str): suffix to append to task name. Default is empty string.
        input_files (list[str]): a  list of input files
        mounts (list[tuple]): a list of tuples in the form (container_name, relative_mount_directory)
        depends_on (str | list[str]): list of tasks this task depends on
        run_dependent_tasks_on_fail (bool): whether to run dependent tasks if the parent task fails. Default is False.
        batch_client (object): batch client object
        full_container_name (str): name ACR container to run task on
        task_id_max (int): current max task id in use by Batch

    Returns:
        list: list of task IDs created
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

    no_exit_options = ExitOptions(
        dependency_action=DependencyAction.satisfy,
        job_action=JobAction.none
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
            dependency_action=DependencyAction.block, job_action=JobAction.none,
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
            t = dt.now(zi("America/New_York"))
            s_time = t.strftime("%Y%m%d_%H%M%S")
            if not save_logs_rel_path.startswith("/"):
                save_logs_rel_path = "/" + save_logs_rel_path
            _folder = f"{save_logs_rel_path}/{logs_folder}/"
            sout = f"{_folder}/stdout_{job_id}_{task_id}_{s_time}.txt"
            full_cmd = f"""/bin/bash -c "mkdir -p {_folder}; {d_cmd_str} 2>&1 | tee {sout}" """
    else:
        full_cmd = d_cmd_str

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
                command_line=d_cmd_str + " " + input_file,
                container_settings=batchmodels.TaskContainerSettings(
                    image_name=full_container_name,
                    container_run_options=f"--name={job_id} --rm " + mount_str,
                ),
                user_identity=user_identity,
                depends_on=task_deps,
                exit_conditions=exit_conditions,
            )
            batch_client.task.add(job_id=job_id, task=task)
            print(f"Task '{id}' added to job '{job_id}'.")
        return tasks
    else:
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
        )
        batch_client.task.add(job_id=job_id, task=task)
        logger.debug(f"Task '{task_id}' added to job '{job_id}'.")
        t = []
        t.append(task_id)
        return t


def monitor_tasks(
    job_id: str,
    timeout: int,
    batch_client: object,
    resource_group,
    account_name,
    pool_name,
    batch_mgmt_client,
):
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

    if completed:
        logger.info(
            "All tasks have reached 'Completed' state within the timeout period."
        )
        logger.info(f"{successes} task(s) succeeded, {failures} failed.")
    else:
        raise RuntimeError(
            f"ERROR: Tasks did not reach 'Completed' state within timeout period of {timeout} minutes."
        )

    end_time = datetime.datetime.now().replace(microsecond=0)
    runtime = end_time - start_time
    logger.info(
        f"Monitoring ended: {end_time}. Total elapsed time: {runtime}."
    )
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
    logger.info(f"Listing files in container '{container_name}'...")
    try:
        cc = ContainerClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=sp_credential,
            container_name=container_name,
        )
        files = [f for f in cc.list_blob_names()]
        logger.info(files)
        logger.info(
            f"Found {len(files)} files in container '{container_name}'."
        )
        return files
    except Exception as e:
        logger.error(f"Error connecting to container '{container_name}': {e}")
        raise e


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


def get_user_identity(config: str):
    """gets the user identity based on the config information.

    Args:
        config (str): config dict

    Returns:
        dict: the dictionary containing user identity information to be used with the pool parameters.
    """
    logger.debug("Getting user identity configuration.")
    user_identity = {
        "type": "UserAssigned",
        "userAssignedIdentities": {
            config["Authentication"]["user_assigned_identity"]: {
                "clientId": config["Authentication"]["batch_application_id"],
                "principalId": config["Authentication"]["batch_object_id"],
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
    logger.debug("Getting network configuration.")
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
    availability_zones: bool = False,
    use_hpc_image: bool = False,
):
    """gets the deployment config based on the config information

    Args:
        container_image_name (str): container image name
        container_registry_url (str): container registry URL
        container_registry_server (str): container registry server
        config (str): config dict
        availability_zones (bool): whether to use availability zones. Default False.
        use_hpc_image (bool): whether to use high performance compute images for each node. Default False.

    Returns:
        dict: dictionary containing info for container deployment. Uses ubuntu server with info obtained from config file.
    """
    logger.debug("setting availability zone info")
    if availability_zones:
        policy = "Zonal"
    else:
        policy = "Regional"

    if use_hpc_image:
        image_ref = {
            "publisher": "microsoft-dsvm",
            "offer": "ubuntu-hpc",
            "sku": "2204",
            "version": "latest",
        }
        node_agent_sku = "batch.node.ubuntu 22.04"
    else:
        image_ref = {
            "publisher": "microsoft-azure-batch",
            "offer": "ubuntu-server-container",
            "sku": "20-04-lts",
            "version": "latest",
        }
        node_agent_sku = "batch.node.ubuntu 20.04"

    logger.debug("Getting deployment config.")
    deployment_config = {
        "virtualMachineConfiguration": {
            "imageReference": image_ref,
            "nodePlacementConfiguration": {"policy": policy},
            "nodeAgentSkuId": node_agent_sku,
            "containerConfiguration": {
                "type": "dockercompatible",
                "containerImageNames": [container_image_name],
                "containerRegistries": [
                    {
                        "registryUrl": container_registry_url,
                        "userName": config["Authentication"][
                            "sp_application_id"
                        ],
                        "password": get_sp_secret(config),
                        "registryServer": container_registry_server,
                    }
                ],
            },
        }
    }
    return deployment_config


def get_blob_config(
    container_name: str,
    rel_mount_path: str,
    cache_blobfuse: bool,
    config: dict,
):
    """gets the blob storage configuration based on the config information

    Args:
        container_name (str): name of Blob Storage Container
        rel_mount_path (str): relative mount path
        cache_blobfuse (bool): True to use blobfuse caching, False to download data from blobfuse every time
        config (dict): config dict

    Returns:
        dict: dictionary containing info for blob storage configuration. Used as input to get_mount_config().
    """
    logger.debug(
        f"Generating blob configuration for container '{container_name}' with mount path '{rel_mount_path}'..."
    )
    if cache_blobfuse:
        blob_str = ""
        logger.debug("No Blob caching in use.")
    else:
        blob_str = "-o direct_io"
        logger.debug("Will use blob caching.")

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
    logger.debug("Generated Blob configuration.")
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
    logger.debug("Generated mount configuration.")
    return _mount_config


def get_pool_parameters(
    mode: str,
    container_image_name: str,
    container_registry_url: str,
    container_registry_server: str,
    config: dict,
    mount_config: list,
    autoscale_formula_path: str = None,
    autoscale_evaluation_interval: str = "PT5M",
    timeout: int = 60,
    dedicated_nodes: int = 0,
    low_priority_nodes: int = 1,
    use_default_autoscale_formula: bool = False,
    max_autoscale_nodes: int = 3,
    task_slots_per_node: int = 1,
    availability_zones: bool = False,
    use_hpc_image: bool = False,
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
        max_autoscale_nodes (int): maximum number of nodes to use with autoscaling. Default 3.
        task_slots_per_node (int): number of task slots per node. Default is 1.
        use_hpc_image (bool): whether to use a high performance compute image for each node. Default False.

    Returns:
        dict: dict of pool parameters for pool creation
    """
    logger.debug(
        f"Setting up pool parameters in '{mode}' mode with timeout={timeout} minutes..."
    )
    fixedscale_resize_timeout = "PT15M"
    if mode == "fixed":
        fixedscale_resize_timeout = f"PT{timeout}M"
        scale_settings = {
            "fixedScale": {
                "targetDedicatedNodes": dedicated_nodes,
                "targetLowPriorityNodes": low_priority_nodes,
                "resizeTimeout": fixedscale_resize_timeout,
            }
        }
        logger.debug("Fixed mode set with scale settings.")
    elif mode == "autoscale" and use_default_autoscale_formula is False:
        scale_settings = {
            "autoScale": {
                "evaluationInterval": autoscale_evaluation_interval,
                "formula": get_autoscale_formula(
                    filepath=autoscale_formula_path
                ),
            }
        }
        logger.debug("Autoscale mode set with custom autoscale formula.")
    elif mode == "autoscale" and use_default_autoscale_formula is True:
        scale_settings = {
            "autoScale": {
                "evaluationInterval": autoscale_evaluation_interval,
                "formula": generate_autoscale_formula(
                    max_nodes=max_autoscale_nodes
                ),
            }
        }
        logger.debug("Autoscale mode set with default autoscale formula.")
    else:
        logger.debug("Returning empty pool parameters.")
        return {}

    pool_parameters = {
        "identity": get_user_identity(config),
        "properties": {
            "vmSize": config["Batch"]["pool_vm_size"],
            "interNodeCommunication": "Disabled",
            "taskSlotsPerNode": task_slots_per_node,
            "taskSchedulingPolicy": {"nodeFillType": "Spread"},
            "deploymentConfiguration": get_deployment_config(
                container_image_name,
                container_registry_url,
                container_registry_server,
                config,
                availability_zones,
                use_hpc_image,
            ),
            "networkConfiguration": get_network_config(config),
            "scaleSettings": scale_settings,
            "resizeOperationStatus": {
                "targetDedicatedNodes": 1,
                "nodeDeallocationOption": "Requeue",
                "resizeTimeout": fixedscale_resize_timeout,
                "startTime": "2023-07-05T13:18:25.7572321Z",
            },
            "currentDedicatedNodes": 1,
            "currentLowPriorityNodes": 0,
            "targetNodeCommunicationMode": "Simplified",
            "currentNodeCommunicationMode": "Simplified",
            "mountConfiguration": mount_config,
        },
    }
    logger.debug("Pool parameters successfully configured.")
    return pool_parameters


def check_blob_existence(c_client: ContainerClient, blob_name: str) -> bool:
    """Checks whether a blob exists in the specified container

    Args:
        c_client (ContainerClient): an Azure Container Client object
        blob_name (str): name of Blob to check for existence

    Returns:
        bool: whether the specified Blob exists

    """
    logger.debug("Checking Blob existence.")
    blob = c_client.get_blob_client(blob=blob_name)
    logger.debug(f"Blob exists: {blob.exists()}")
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
        logger.debug(f"{first_blob.name} found.")
        return True
    except StopIteration as e:
        logger.error(repr(e))
        raise e


def download_file(
    c_client: ContainerClient,
    src_path: str,
    dest_path: str,
    do_check: bool = True,
    verbose=False,
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
        logger.debug("File downloaded.")
        if verbose:
            print(f"Downloaded {src_path} to {dest_path}")


def download_directory(
    container_name: str,
    src_path: str,
    dest_path: str,
    blob_service_client,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    verbose=True,
) -> None:
    """
    Downloads a directory using prefix matching and the .list_blobs() method

    Args:
        container_name (str):
            name of Blob container
        src_path (str):
            Prefix of the blobs to download
        dest_path (str):
            Path to the directory in which to store the downloads
        blob_service_client (BlobServiceClient):
            instance of BlobServiceClient
        include_extensions (str, list, None):
            a string or list of extensions to include in the download. Optional.
        exclude_extensions (str, list, None):
            a string of list of extensions to exclude from the download. Optional.
        verbose (bool):
            a Boolean whether to print file names when downloaded.

    Raises:
        ValueError:
            When no blobs exist with the specified prefix (src_path)
    """
    # check that include and exclude extensions are not both used, format if exist
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    elif exclude_extensions is not None:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error(
            "Use included_extensions or exclude_extensions, not both."
        )
        print("Use included_extensions or exclude_extensions, not both.")
        raise Exception(
            "Use included_extensions or exclude_extensions, not both."
        ) from None
    # check container exists
    logger.debug(f"Checking Blob container {container_name} exists.")
    # create container client
    c_client = blob_service_client.get_container_client(
        container=container_name
    )
    if not check_virtual_directory_existence(c_client, src_path):
        raise ValueError(
            f"Source virtual directory: {src_path} does not exist."
        )

    blob_list = []
    if not src_path.endswith("/"):
        src_path += "/"
    for blob in c_client.list_blobs(name_starts_with=src_path):
        b = blob.name
        if b.split(src_path)[0] == "":
            blob_list.append(b)

    flist = []
    if include_extensions is None:
        if exclude_extensions is not None:
            # find files that don't contain the specified extensions
            for _file in blob_list:
                if os.path.splitext(_file)[1] not in exclude_extensions:
                    flist.append(_file)
        else:  # this is for no specified extensions to include or exclude
            flist = blob_list
    else:
        # include only specified extension files
        for _file in blob_list:
            if os.path.splitext(_file)[1] in include_extensions:
                flist.append(_file)
    for blob in flist:
        download_file(
            c_client, blob, os.path.join(dest_path, blob), False, verbose
        )
    logger.debug("Download complete.")


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
        tag (str): tag for the Docker container
        path_to_dockerfile (str): path to Dockerfile. Default is ./Dockerfile.
        use_device_code (bool): whether to use the device code when authenticating. Default False.

    Returns:
        str: full container name
    """
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

    logger.debug("pulling list of docker images available.")
    d_list = [image.tags for image in docker_env.images.list()]
    logger.debug("checking if image_name exists in docker repo.")
    status = sum([image_name in image for image in d_list])
    if status == 0:
        logger.error(
            f"Image {image_name} does not exist. Check the image name."
        )
        print(f"Image {image_name} does not exist. Check the image name.")
        raise Exception(
            f"Image {image_name} does not exist. Check the image name."
        ) from None
    else:
        logger.debug(f"{image_name} found in docker repo.")
    full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"

    # tag the image with full_container_name
    logger.debug(f"tagging {image_name} before pushing to ACR.")
    sp.run(f"docker tag {image_name} {full_container_name}", shell=True)
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
    logger.debug(f"Checking if pool {pool_name} exists.")
    try:
        batch_mgmt_client.pool.get(
            resource_group_name, account_name, pool_name
        )
        logger.debug("Pool exists.")
        return True
    except Exception:
        logger.debug("Pool does not exist.")
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
    logger.debug("Pulling pool info.")
    result = batch_mgmt_client.pool.get(
        resource_group_name, account_name, pool_name
    )
    logger.debug("Condensing pool info the readable json output.")
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
    logger.debug("Pulling pool info.")
    result = batch_mgmt_client.pool.get(
        resource_group_name, account_name, pool_name
    )
    return result


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
        return True
    else:
        logger.warning(
            str(list(req - loaded)),
            "missing from the config file and will be required by client.",
        )
        return False


def get_container_registry_client(endpoint: str, audience: str):
    return ContainerRegistryClient(
        endpoint, DefaultAzureCredential(), audience=audience
    )


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
    logger.debug(f"Set audience to {audience}")
    logger.debug(f"Set endpoint to {endpoint}")
    try:
        # check full_container_name exists in ACR
        cr_client = get_container_registry_client(
            endpoint=endpoint, audience=audience
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


def generate_autoscale_formula(max_nodes: int = 8) -> str:
    """
    Generate a generic autoscale formula for use based on maximum number of nodes to scale up to.

    Args:
        max_nodes (int): maximum number of nodes to cap the pool at

    Returns:
        str: the text of an autoscale formula

    """
    logger.debug("Creating default autoscale formula.")
    formula = f"""
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
        logger.debug(f"path formatted to {rel_path}")
    return rel_path


def get_timeout(_time: str) -> int:
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


def list_blobs_flat(
    container_name: str, blob_service_client: BlobServiceClient, verbose=True
):
    logger.debug("Creating container client for getting Blob info.")
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    logger.debug("Container client created. Listing Blob info.")
    blob_list = container_client.list_blobs()
    blob_names = [blob.name for blob in blob_list]
    logger.debug("Blob names gathered.")
    if verbose:
        for blob in blob_list:
            logger.info(f"Name: {blob.name}")
    return blob_names


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


def delete_blob_snapshots(
    blob_name: str, container_name: str, blob_service_client: object
):
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    blob_client.delete_blob(delete_snapshots="include")
    logger.info(f"Deleted {blob_name} from {container_name}.")


def delete_blob_folder(
    folder_path: str, container_name: str, blob_service_client: object
):
    # create container client
    c_client = blob_service_client.get_container_client(
        container=container_name
    )
    # list out files in folder
    blob_names = c_client.list_blob_names(name_starts_with=folder_path)
    _files = [blob for blob in blob_names]
    # call helpers.delete_blob_snapshots()
    for file in _files:
        delete_blob_snapshots(
            blob_name=file,
            container_name=container_name,
            blob_service_client=blob_service_client,
        )


def format_extensions(extension):
    if isinstance(extension, str):
        extension = [extension]
    ext = []
    for _ext in extension:
        if _ext.startswith("."):
            ext.append(_ext)
        else:
            ext.append("." + _ext)
    return ext


def mark_job_completed_after_tasks_run(
    job_id: str,
    pool_id: str,
    batch_client: object,
    mark_complete: bool = True,
):
    if mark_complete:
        logger.debug("setting terminateJob when all tasks complete for job.")
        job_term = batchmodels.JobUpdateParameter(
            id=job_id,
            pool_info=batchmodels.PoolInformation(pool_id=pool_id),
            on_all_tasks_complete="terminateJob",
        )
        batch_client.job.update(job_id=job_id, job_update_parameter=job_term)
        print(
            "Job will be marked complete when all tasks finish, even if task(s) fails."
        )


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


def get_rel_mnt_path(
    blob_name: str,
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    try:
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        return "ERROR!"
    mc = pool_info.as_dict()["mount_configuration"]
    for m in mc:
        if (
            m["azure_blob_file_system_configuration"]["container_name"]
            == blob_name
        ):
            rel_mnt_path = m["azure_blob_file_system_configuration"][
                "relative_mount_path"
            ]
            return rel_mnt_path
    logger.error(f"could not find blob {blob_name} mounted to pool.")
    print(f"could not find blob {blob_name} mounted to pool.")
    return "ERROR!"


def get_pool_mounts(
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    try:
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        print(f"could not retrieve pool info for {pool_name}.")
        return None
    mounts = []
    mc = pool_info.as_dict()["mount_configuration"]
    for m in mc:
        mounts.append(
            (
                m["azure_blob_file_system_configuration"]["container_name"],
                m["azure_blob_file_system_configuration"][
                    "relative_mount_path"
                ],
            )
        )
    return mounts
