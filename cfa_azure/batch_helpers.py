# import modules for use
import argparse
import csv
import datetime
import fnmatch as fm
import json
import logging
import os
import re
import subprocess as sp
import time
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
    MetadataItem,
    OnAllTasksComplete,
    OnTaskFailure,
)
from azure.containerregistry import ContainerRegistryClient
from azure.core.exceptions import HttpResponseError
from azure.core.paging import ItemPaged
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.batch import BatchManagementClient
from azure.storage.blob import (
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)
from docker.errors import DockerException
from griddler import griddle
from yaml import SafeLoader, dump, load

logger = logging.getLogger(__name__)


def get_batch_mgmt_client(config: dict, credential: object):
    """establishes a Batch Management Client based on credentials and config file

    Args:
        config (dict): config dictionary
        credential (object): credential object from azure.identity

    Returns:
        class: an instance of the Batch Management Client
    """
    logger.debug("Initializing Batch Management Client...")
    try:
        batch_mgmt_client = BatchManagementClient(
            credential=credential,
            subscription_id=config["Authentication"]["subscription_id"],
        )
        logger.debug("Batch Management Client successfully created.")
        return batch_mgmt_client
    except KeyError as e:
        logger.warning(
            f"Configuration error: '{e}' does not exist in the config file. Please add it to the Authentication section.",
        )
        print(
            f"WARNING creating BatchManagementClient: Configuration error: '{e}' does not exist in the config file. Please add it to the Authentication section if necessary."
        )
        print("Some functionality may be unavailable.")
        return None


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
        autoscale_evaluation_interval (str): time period for autoscale evaluation. Default is 15 minutes.
        fixedscale_resize_timeout (str): timeout for resizing fixed scale pools. Default is 15 minutes.

    Returns:
        dict: relevant information for Batch pool creation
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
    vm_size = config["Batch"]["pool_vm_size"]
    if vm_size.split("_")[1][0].upper() == "A":
        print(
            "Cannot use A-series VMs with new image. Setting standard_D4s_v3 as VM to use."
        )
        print(
            "If another VM is desired, please change it in your config.toml."
        )
        vm_size = "standard_D4s_v3"

    # Assemble the pool parameters JSON
    logger.debug("Generating autoscale formula...")
    pool_parameters = {
        "identity": user_identity,
        "properties": {
            "vmSize": vm_size,
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


def get_pool_parameters(
    mode: str,
    container_image_name: str,
    container_registry_url: str,
    container_registry_server: str,
    config: dict,
    mount_config: list,
    credential: object,
    autoscale_formula_path: str = None,
    autoscale_evaluation_interval: str = "PT5M",
    timeout: int = 60,
    dedicated_nodes: int = 0,
    low_priority_nodes: int = 1,
    use_default_autoscale_formula: bool = False,
    max_autoscale_nodes: int = 3,
    task_slots_per_node: int = 1,
    availability_zones: bool = False,
):
    """creates a pool parameter dictionary to be used with pool creation.

    Args:
        mode (str): either 'fixed' or 'autoscale'
        container_image_name (str): container image name
        container_registry_url (str): container registry URL
        container_registry_server (str): container registry server
        config (dict): config dict
        mount_config (list): output from get_mount_config() regarding mounting of blob storage
        credential (object): credential object from azure.identity
        autoscale_formula_path (str, optional): path to autoscale formula file if mode is 'autoscale'. Defaults to None.
        timeout (int, optional): length in minutes of timeout for tasks that run in this pool. Defaults to 60.
        dedicated_nodes (int, optional): number of dedicated nodes. Defaults to 1.
        low_priority_nodes (int, optional): number of low priority nodes. Defaults to 0.
        use_default_autoscale_formula (bool, optional)
        max_autoscale_nodes (int): maximum number of nodes to use with autoscaling. Default 3.
        task_slots_per_node (int): number of task slots per node. Default is 1.
        availability_zones (bool): whether to use availability zones. Default False.

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

    # check task_slots_per_node
    vm_size = config["Batch"]["pool_vm_size"]
    if vm_size.split("_")[1][0].upper() == "A":
        print(
            "\nCannot use A-series VMs with new image. Setting standard_D4s_v3 as VM to use."
        )
        print(
            "If another VM is desired, please change it in your config.toml.\n"
        )
        vm_size = "standard_D4s_v3"
    task_slots = check_tasks_v_cores(
        task_slots=task_slots_per_node, vm_size=vm_size
    )
    pool_parameters = {
        "identity": get_user_identity(config),
        "properties": {
            "vmSize": vm_size,
            "interNodeCommunication": "Disabled",
            "taskSlotsPerNode": task_slots,
            "taskSchedulingPolicy": {"nodeFillType": "Spread"},
            "deploymentConfiguration": get_deployment_config(
                container_image_name=container_image_name,
                container_registry_url=container_registry_url,
                container_registry_server=container_registry_server,
                config=config,
                credential=credential,
                availability_zones=availability_zones,
            ),
            "networkConfiguration": get_network_config(config),
            "scaleSettings": scale_settings,
            "resizeOperationStatus": {
                "targetDedicatedNodes": 1,
                "nodeDeallocationOption": "Requeue",
                "resizeTimeout": fixedscale_resize_timeout,
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


def generate_autoscale_formula(max_nodes: int = 5) -> str:
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
    $targetVMs = $tasks > 0 ? $tasks : max(0, $TargetLowPriorityNodes / 2);
    // The pool size is capped to max_nodes input
    cappedPoolSize = {max_nodes};
    $TargetLowPriorityNodes = max(0, min($targetVMs, cappedPoolSize));
    // Set node deallocation mode - keep nodes active only until tasks finish
    $NodeDeallocationOption = taskcompletion;
    """
    return formula


def check_tasks_v_cores(task_slots: int, vm_size: str) -> int:
    """
    Validates that task slots per node is valid based on VM size. Returns the correct task slots.

    Args:
        task_slots (int): number of task slots per node
        vm_size (str): vm size for nodes. Usually in the form "standard_D4s_v3".

    Returns:
        int: the correct number of task slots per node based on restrictions of vm_size
    """
    cores = int(re.findall("\d+", vm_size.split("_")[1])[0])
    if task_slots == 1:
        return 1
    elif task_slots < 1:
        print(
            "Task slots per node must be a positive number of at most 256. Setting value of 1."
        )
        return 1
    elif task_slots > 256:
        max_task_slots = min(4 * cores, 256)
        print(
            "Cannot have more than 256 tasks per node. Setting to",
            max_task_slots,
            ".",
        )
        return max_task_slots
    else:
        if task_slots > 4 * cores:
            max_task_slots = 4 * cores
            print(
                task_slots,
                "is over the maximum task slots allowed per node. Setting to",
                max_task_slots,
                ".",
            )
            return max_task_slots
        else:
            return task_slots


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
    credential: object,
    availability_zones: bool = False,
):
    """gets the deployment config based on the config information

    Args:
        container_image_name (str): container image name
        container_registry_url (str): container registry URL
        container_registry_server (str): container registry server
        config (str): config dict
        credential (object): credential object
        availability_zones (bool): whether to use availability zones. Default False.

    Returns:
        dict: dictionary containing info for container deployment. Uses ubuntu server with info obtained from config file.
    """
    logger.debug("setting availability zone info")
    if availability_zones:
        policy = "Zonal"
    else:
        policy = "Regional"

    image_ref = {
        "publisher": "microsoft-dsvm",
        "offer": "ubuntu-hpc",
        "sku": "2204",
        "version": "latest",
    }
    node_agent_sku = "batch.node.ubuntu 22.04"

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
                        "password": get_sp_secret(config, credential),
                        "registryServer": container_registry_server,
                    }
                ],
            },
        }
    }
    return deployment_config
