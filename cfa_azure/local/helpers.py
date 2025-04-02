# import modules for use
import logging
import os
import re
import shutil
import subprocess as sp
from os import path, walk
from pathlib import Path

import docker
import pandas as pd
import toml
from docker.errors import DockerException
from griddler import griddle

from cfa_azure.local import batch

logger = logging.getLogger(__name__)


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
    if isinstance(credential, str):
        return "secret"
    logger.debug("Attempting to establish secret client.")
    try:
        config["Authentication"]["vault_url"]
        logger.debug("Secret client initialized.")
    except KeyError as e:
        logger.error("Error:", e, "Key not found in configuration.")
        raise e

    logger.debug("Attempting to retrieve Service Principal secret.")
    try:
        sp_secret = config["Authentication"]["vault_sp_secret_id"]
        logger.debug("Service principal secret successfully retrieved.")
        return sp_secret
    except Exception as e:
        logger.error("Error retrieving secret:", e)
        logger.warning(
            "Check that vault_uri and vault_sp_secret_id are correctly configured in the config file."
        )
        raise e


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
        blob_config (list): usually from get_blob_config(). Usually one for input blob and one for output blob.

    Returns:
        list: mount configuration to used with get_pool_parameters.
    """
    _mount_config = []
    for blob in blob_config:
        _mount_config.append(blob)
    logger.debug("Generated mount configuration.")
    return _mount_config


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
            logger.debug("Device code used here for az login.")
        else:
            logger.debug("Logging in to Azure with az login.")
        logger.debug("Pushing Docker container to ACR.")
        print(f"Built {full_container_name}")
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
    else:
        logger.debug("Logging in to Azure.")
        sp.run("az login", shell=True)
    logger.debug("Pushing Docker container to ACR.")
    logger.debug("Container should have been uploaded.")
    return full_container_name


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
        logger.debug("Container registry client would have been created.")
    except Exception as e:
        logger.error(
            f"Registry [{registry_name}] or repo [{repo_name}] does not exist"
        )
        logger.exception(e)
        raise e
    logger.debug("Checking tag exists.")
    image_name = f"{registry_name}.azurecr.io/{repo_name}:{tag_name}"
    docker_env = docker.from_env(timeout=8)
    try:
        docker_env.images.get(image_name)
    except docker.errors.ImageNotFound:
        # Log available images to guide the user
        available_images = [img.tags for img in docker_env.images.list()]
        logger.error(
            f"Image {image_name} does not exist. Available images are: {available_images}"
        )
        raise
    return image_name


def create_container(container_name: str, blob_service_client: object):
    """creates a Blob container if not exists

    Args:
        container_name (str): user specified name for Blob container
        blob_service_client (object): BlobServiceClient object

    Returns:
       object: ContainerClient object
    """
    logger.debug(f"Attempting to create or access container: {container_name}")
    try:
        os.makedirs(container_name)
        logger.info(f"Container [{container_name}] created successfully.")
    except Exception:
        logger.debug(
            f"Container [{container_name}] already exists. No action needed."
        )
    return "container_client"


def upload_blob_file(
    filepath: str,
    location: str = "",
    container_name: object = None,
    verbose: bool = False,
):
    """Uploads a specified file to Blob storage.
    Args:
        filepath (str): the path to the file.
        location (str): the location (folder) inside the Blob container. Uploaded to root if "". Default is "".
        container_name: name of Blob container (local).
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
    # check container exists
    if not os.path.exists(container_name):
        logger.error("container does not exist")
        return None
    else:
        logger.debug("Container exists")

    _, _file = path.split(filepath)
    _name = path.join(location, _file)
    print(_name)
    src = Path(filepath)
    dest = Path(f"{container_name}/{_name}")
    os.makedirs(f"{container_name}/{location}", exist_ok=True)
    # copy file to container
    shutil.copy2(src, dest)
    if verbose:
        print(f"Uploaded {filepath} to {container_name} as {_name}.")
        logger.info(f"Uploaded {filepath} to {container_name} as {_name}.")


def upload_files_in_folder(
    folder: str,
    container_name: str,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    location_in_blob: str = "",
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
    if not os.path.exists(container_name):
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
            container_name,
            verbose,
        )
    return file_list


def format_extensions(extension):
    """
    Formats extensions to include periods.
    Args:
        extension (str | list): string or list of strings of extensions. Can include a leading period but does not need to.

    Returns:
        list: list of formatted extensions
    """
    if isinstance(extension, str):
        extension = [extension]
    ext = []
    for _ext in extension:
        if _ext.startswith("."):
            ext.append(_ext)
        else:
            ext.append("." + _ext)
    return ext


def walk_folder(folder: str) -> list | None:
    """
    Args:
        folder (str): folder path

    Returns:
        list: list of file names contained in folder
    """
    file_list = []
    for dirname, _, fname in walk(folder):
        for f in fname:
            _path = path.join(dirname, f)
            file_list.append(_path)
    return file_list


def add_job(
    job_id: str,
    pool_id: str,
    task_retries: int = 0,
    mark_complete: bool = False,
):
    """takes in a job ID and config to create a job in the pool

    Args:
        job_id (str): name of the job to run
        pool_id (str): name of pool
        task_retries (int): number of times to retry the task if it fails. Default 3.
        mark_complete (bool): whether to mark the job complete after tasks finish running. Default False.
    """
    logger.debug(f"Attempting to create job '{job_id}'...")
    logger.debug("Attempting to add job.")
    j = batch.Job(job_id, pool_id, task_retries, mark_complete)
    # save job info
    os.makedirs("tmp/jobs", exist_ok=True)
    logger.debug("saving to tmp/jobs folder")
    save_path = Path(f"tmp/jobs/{job_id}.txt")
    save_path.write_text(f"{pool_id} {task_retries} {mark_complete}")
    return j


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


def get_args_from_yaml(file_path: str) -> list[str]:
    """
    parses yaml file and returns list of strings containing command line arguments and flags captured in the yaml.

    Args:
        file_path (str): path to yaml file

    Returns:
        list[str]: list of command line arguments
    """
    parameter_sets = griddle.read(file_path)
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


def check_pool_exists(
    pool_name: str,
):
    """Check if a pool exists in Azure Batch

    Args:
        resource_group_name (str): Azure resource group name
        account_name (str): Azure account name
        pool_name (str): name of pool
        batch_mgmt_client (object): instance of BatchManagementClient

    Returns:
        bool: whether the pool exists
    """
    logger.debug(f"Checking if pool {pool_name} exists.")
    if os.path.exists(f"tmp/pools/{pool_name}.txt"):
        logger.debug("Pool exists.")
        return True
    else:
        logger.debug("Pool does not exist.")
        return False
