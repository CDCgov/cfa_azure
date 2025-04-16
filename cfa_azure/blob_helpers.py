# import modules for use
import argparse
import fnmatch as fm
import logging
import os
from os import path, walk
from pathlib import Path

from azure.core.paging import ItemPaged
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    StorageStreamDownloader,
)

from cfa_azure import helpers

logger = logging.getLogger(__name__)


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
    blobs = list_blobs_in_container(
        name_starts_with=vdir_path, container_client=c_client
    )
    try:
        first_blob = next(blobs)
        logger.debug(f"{first_blob.name} found.")
        return True
    except StopIteration as e:
        logger.error(repr(e))
        raise e


def write_blob_stream(
    data,
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
) -> bool:
    """
    Write a stream into a file in Azure Blob storage

    Args:
        data (stream):
            [Required] File contents as stream
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
        account_name (str):
            [Optional] Name of Azure storage account
        container_name (str):
            [Optional] Name of Blob container within storage account
        container_client (ContainerClient):
            [Optional] Instance of ContainerClient provided with the storage account

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    if container_client:
        pass
    elif container_name and account_name:
        config = {
            "Storage": {
                "storage_account_url": f"https://{account_name}.blob.core.windows.net"
            }
        }
        blob_service_client = get_blob_service_client(
            config=config, credential=DefaultAzureCredential()
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    else:
        raise ValueError(
            "Either container name and account name or container client must be provided."
        )
    container_client.upload_blob(name=blob_url, data=data, overwrite=True)
    return True


def infer_prefix_split(blob_url: str) -> str:
    """
    Determine prefix both from Blob Url

    Args:
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
    """
    blob_url_parts = blob_url.split("*", maxsplit=1)
    data_path_candidate, suffix_pattern = (
        (blob_url_parts[0], blob_url_parts[1:])
        if len(blob_url_parts) == 2
        else (blob_url_parts[0], "")
    )
    if data_path_candidate and not data_path_candidate.endswith("/"):
        data_path_candidate_parts = data_path_candidate.split("/", maxsplit=1)
        if len(data_path_candidate_parts) > 0:
            data_path = f"{data_path_candidate_parts[0]}/"
            file_pattern = (
                "/".join(data_path_candidate_parts[1:])
                + "*"
                + "".join(suffix_pattern)
            )
        else:
            data_path = data_path_candidate
            file_pattern = data_path_candidate
    else:
        data_path = data_path_candidate
        file_pattern = "*" + "".join(suffix_pattern)
    return data_path, file_pattern


def blob_search(
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
    **kwargs,
) -> ItemPaged[BlobProperties]:
    """
    List all blobs in specified container along with their metadata

    Args:
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
        account_name (str):
            [Optional] Name of Azure storage account
        container_name (str):
            [Optional] Name of Blob container within storage account
        container_client (ContainerClient):
            [Optional] Instance of ContainerClient provided with the storage account
        sort_key (str):
            [Optional]: Blob property to use for sorting the result in ascending order

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    data_path, file_pattern = infer_prefix_split(blob_url)
    subset_files = list_blobs_in_container(
        name_starts_with=data_path,
        account_name=account_name,
        container_name=container_name,
        container_client=container_client,
    )
    filtered_subset = iter(
        filter(
            lambda blob: fm.fnmatch(blob["name"], f"{file_pattern}"),
            subset_files,
        )
    )
    sort_key = kwargs.get("sort_key")
    if sort_key:
        filtered_subset = sorted(
            filtered_subset, key=lambda blob: blob[sort_key]
        )
    return filtered_subset


def blob_glob(
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
) -> ItemPaged[BlobProperties]:
    """
    List all blobs in specified container

    Args:
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
        account_name (str):
            [Optional] Name of Azure storage account
        container_name (str):
            [Optional] Name of Blob container within storage account
        container_client (ContainerClient):
            [Optional] Instance of ContainerClient provided with the storage account

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    data_path, file_pattern = infer_prefix_split(blob_url)
    subset_files = walk_blobs_in_container(
        name_starts_with=data_path,
        account_name=account_name,
        container_name=container_name,
        container_client=container_client,
    )
    filtered_subset = filter(
        lambda blob: fm.fnmatch(blob["name"], f"{file_pattern}"),
        subset_files,
    )
    return filtered_subset


def get_container_client(account_name: str, container_name: str):
    """
    Instantiate container client using account name and container name
    """
    config = {
        "Storage": {
            "storage_account_url": f"https://{account_name}.blob.core.windows.net"
        }
    }
    blob_service_client = get_blob_service_client(
        config=config, credential=DefaultAzureCredential()
    )
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    return container_client


def read_blob_stream(
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
    do_check: bool = True,
) -> StorageStreamDownloader[str]:
    """
    Download a file from Azure Blob storage and return the contents as stream

    Args:
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
        account_name (str):
            [Optional] Name of Azure storage account
        container_name (str):
            [Optional] Name of Blob container within storage account
        container_client (ContainerClient):
            [Optional] Instance of ContainerClient provided with the storage account
        do_check (bool):
            [Optional] Whether or not to do an existence check

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    if container_client:
        pass
    elif container_name and account_name:
        container_client = get_container_client(account_name, container_name)
    else:
        raise ValueError(
            "Either container name and account name or container client must be provided."
        )

    if do_check and not check_blob_existence(container_client, blob_url):
        raise ValueError(f"Source blob: {blob_url} does not exist.")
    download_stream = container_client.download_blob(blob=blob_url)
    return download_stream


def download_file(
    c_client: ContainerClient,
    src_path: str,
    dest_path: str,
    do_check: bool = True,
    verbose: bool = False,
) -> None:
    """
    Download a file from Azure Blob storage

    Args:
        c_client (ContainerClient):
            Instance of ContainerClient provided with the storage account
        src_path (str):
            Path within the container to the desired file (including filename)
        dest_path (str):
            Path to desired location to save the downloaded file
        container (str):
            Name of the storage container containing the file to be downloaded
        do_check (bool):
            Whether or not to do an existence check
        verbose (bool):
            Whether to be verbose in printing information

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    download_stream = read_blob_stream(
        src_path, container_client=c_client, do_check=do_check
    )
    dest_path = Path(dest_path)
    dest_path.parents[0].mkdir(parents=True, exist_ok=True)
    with dest_path.open(mode="wb") as blob_download:
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
    for blob in list_blobs_in_container(
        name_starts_with=src_path, container_client=c_client
    ):
        b = blob.name
        if b.split(src_path)[0] == "" and "." in b:
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


def get_blob_service_client(config: dict, credential: object):
    """establishes Blob Service Client using credentials

    Args:
        config (dict): contains configuration info
        credential (object): credential object from aazure.identity

    Returns:
        class: an instance of BlobServiceClient
    """
    logger.debug("Initializing Blob Service Client...")
    try:
        blob_service_client = BlobServiceClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=credential,
        )
        logger.debug("Blob Service Client successfully created.")
        return blob_service_client
    except KeyError as e:
        logger.error(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Storage section.",
        )
        raise e


def create_blob_containers(
    blob_service_client: BlobServiceClient,
    input_container_name: str = None,
    output_container_name: str = None,
):
    """creates the input and output Blob containers based on given names

    Args:
        blob_service_client (object): an instance of the Blob Service Client
        input_container_name (str): user specified name for input container. Default is None.
        output_container_name (str): user specified name for output container. Default is None.
    """
    # print("Preparing to create blob containers...")
    if input_container_name:
        logger.info(
            f"Attempting to create input container: '{input_container_name}'..."
        )
        helpers.create_container(input_container_name, blob_service_client)
    else:
        logger.warning(
            "Input container name not specified. Skipping input container creation."
        )

    if output_container_name:
        logger.info(
            f"Attempting to create output container: '{output_container_name}'..."
        )
        helpers.create_container(output_container_name, blob_service_client)
    else:
        logger.warning(
            "Output container name not specified. Skipping output container creation."
        )


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


def initialize_blob_arguments():
    """
    Initialize command line arguments for copy_blob and write_blob
    """
    parser = argparse.ArgumentParser(
        description="Argument parser for copy_blob"
    )
    parser.add_argument(
        "--account", required=True, type=str, help="Azure Blob Storage Account"
    )
    parser.add_argument(
        "--container", required=True, type=str, help="Blob container"
    )
    parser.add_argument(
        "--blobpath",
        required=True,
        type=str,
        help="Path where data will be uploaded inside container",
    )
    parser.add_argument(
        "--localpath",
        required=True,
        type=str,
        help="Local folder that will be uploaded",
    )
    args = parser.parse_args()
    return args


def download_blob():
    """
    Download a blob from Azure Storage to local file system

    Usage: copy_blob --account '{storage_account}' --container '{storage_container}' --blobpath '{path/in/container}' --localpath '{local/path/for/data}'
    """
    args = initialize_blob_arguments()
    container_client = get_container_client(args.account, args.container)
    download_file(
        c_client=container_client,
        src_path=args.blobpath,
        dest_path=args.localpath,
        do_check=True,
        verbose=True,
    )


def upload_blob():
    """
    Upload a blob from local file system to Azure Storage

    Usage: write_blob --account '{storage_account}' --container '{storage_container}' --blobpath '{path/in/container}' --localpath '{local/path/for/data}'
    """
    args = initialize_blob_arguments()
    container_client = get_container_client(args.account, args.container)
    upload_blob_file(
        filepath=args.localpath,
        location=args.blobpath,
        container_client=container_client,
        verbose=True,
    )


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


def instantiate_container_client(
    container_name: str = None,
    account_name: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
):
    """
    Returns a Container Client
    """
    logger.debug("Creating container client for getting Blob info.")
    if container_client:
        pass
    elif blob_service_client and container_name:
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    elif container_name and account_name:
        config = {
            "Storage": {
                "storage_account_url": f"https://{account_name}.blob.core.windows.net"
            }
        }
        blob_service_client = get_blob_service_client(
            config=config, credential=DefaultAzureCredential()
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    else:
        raise ValueError(
            "Either container name, account name, container client or blob service client must be provided."
        )
    logger.debug("Container client created. Listing Blob info.")
    return container_client


def walk_blobs_in_container(
    container_name: str = None,
    account_name: str = None,
    name_starts_with: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
):
    return instantiate_container_client(
        container_name=container_name,
        account_name=account_name,
        blob_service_client=blob_service_client,
        container_client=container_client,
    ).walk_blobs(name_starts_with)


def list_blobs_in_container(
    container_name: str = None,
    account_name: str = None,
    name_starts_with: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
):
    return instantiate_container_client(
        container_name=container_name,
        account_name=account_name,
        blob_service_client=blob_service_client,
        container_client=container_client,
    ).list_blobs(name_starts_with)


def list_blobs_flat(
    container_name: str, blob_service_client: BlobServiceClient, verbose=True
):
    """
    Args:
        container_name (str): name of Blob container
        blob_service_client (object): instance of BlobServiceClient
        verbose (bool): whether to be verbose in printing files. Default True.

    Returns:
        list: list of blobs in Blob container
    """
    blob_list = list_blobs_in_container(
        container_name=container_name, blob_service_client=blob_service_client
    )
    blob_names = [blob.name for blob in blob_list]
    logger.debug("Blob names gathered.")
    if verbose:
        for blob in blob_list:
            logger.info(f"Name: {blob.name}")
    return blob_names


def list_files_in_container(
    container_name: str, credential: str, config: dict
):
    """lists out files in blob container

    Args:
        container_name (str): the name of the container to get files
        credential (str):  credential object from azure.identity
        config (dict): configuration dictionary

    Returns:
        list: list of file names in the container
    """
    logger.info(f"Listing files in container '{container_name}'...")
    try:
        cc = ContainerClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=credential,
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


def delete_blob_snapshots(
    blob_name: str, container_name: str, blob_service_client: object
):
    """
    Args:
        blob_name (str): name of blob
        container_name (str): name of container
        blob_service_client (object): instance of BlobServiceClient
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    blob_client.delete_blob(delete_snapshots="include")
    logger.info(f"Deleted {blob_name} from {container_name}.")


def delete_blob_folder(
    folder_path: str, container_name: str, blob_service_client: object
):
    """
    Args:
        folder_path (str): path to blob folder
        container_name (str): name of Blob container
        blob_service_client (object): instance of BlobServiceClient
    """
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
