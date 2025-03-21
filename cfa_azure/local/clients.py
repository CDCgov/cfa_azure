import datetime
import logging
import os
import subprocess as sp
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import docker
import pandas as pd

from cfa_azure.local import batch, helpers
from cfa_azure.local.batch import Task

logger = logging.getLogger(__name__)


class AzureClient:
    def __init__(
        self,
        config_path: str | None = None,
        credential_method: str = "identity",
        use_env_vars: bool = False,
    ):
        """Azure Client for interacting with Azure Batch, Container Registries and Blob Storage

        Args:
            config_path (str): path to configuration toml file
            credential_method (str): how to authenticate to Azure. Choices are 'identity', 'sp', and 'env'. Default 'identity'.
            use_env_vars (bool): set to True to load configuration from environment variables

            credential_method details:
                - 'identity' uses managed identity linked to VM
                - 'sp' uses service principal from config/env
                - 'env' uses environment credential based on env variables

        Returns:
            AzureClient object
        """
        self.config_path = config_path
        self.debug = None
        self.files = []
        self.task_id_max = 0
        self.jobs = set()
        self.input_container_name = None
        self.input_mount_dir = None
        self.output_container_name = None
        self.output_mount_dir = None
        self.mounts = []
        self.mount_container_clients = []
        self.timeout = None
        self.save_logs_to_blob = None
        self.logs_folder = "stdout_stderr"
        self.account_name = None
        self.pool_name = None
        self.pool_parameters = None

        logger.debug("Attributes initialized in client.")

        if not config_path and not use_env_vars:
            logger.error(
                "No configuration method specified in initialization."
            )
            raise Exception(
                "No configuration method specified. Please provide a config path or set `use_env_vars=True` to load settings from environment variables."
            )

        # extract credentials using environment variables
        elif use_env_vars:
            try:
                missing_vars = helpers.check_env_req()
                if missing_vars:
                    logger.error(
                        f"Missing the following variables: {missing_vars}."
                    )
                    raise ValueError(
                        f"Missing required environment variables: {', '.join(missing_vars)}"
                    )

                # Construct self.config with a nested structure
                self.config = {
                    "Authentication": {
                        "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
                        "resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
                        "user_assigned_identity": os.getenv(
                            "AZURE_USER_ASSIGNED_IDENTITY"
                        ),
                        "tenant_id": os.getenv("AZURE_TENANT_ID"),
                        "batch_application_id": os.getenv(
                            "AZURE_BATCH_APPLICATION_ID"
                        ),
                        "batch_object_id": os.getenv("AZURE_BATCH_OBJECT_ID"),
                        "sp_application_id": os.getenv(
                            "AZURE_SP_APPLICATION_ID"
                        ),
                        "vault_url": os.getenv("AZURE_VAULT_URL"),
                        "vault_sp_secret_id": os.getenv(
                            "AZURE_VAULT_SP_SECRET_ID"
                        ),
                        "subnet_id": os.getenv("AZURE_SUBNET_ID"),
                    },
                    "Batch": {
                        "batch_account_name": os.getenv(
                            "AZURE_BATCH_ACCOUNT_NAME"
                        ),
                        "batch_service_url": os.getenv(
                            "AZURE_BATCH_SERVICE_URL"
                        ),
                        "pool_vm_size": os.getenv("AZURE_POOL_VM_SIZE"),
                    },
                    "Storage": {
                        "storage_account_name": os.getenv(
                            "AZURE_STORAGE_ACCOUNT_NAME"
                        ),
                        "storage_account_url": os.getenv(
                            "AZURE_STORAGE_ACCOUNT_URL"
                        ),
                    },
                }
                logger.debug(
                    "Config loaded from environment variables with nested structure."
                )
            except ValueError as e:
                logger.error("Environment variable setup failed: %s", e)
                raise e

        else:
            try:
                # load config file
                self.config = helpers.read_config(config_path)
                logger.debug("config loaded")

                config_missing_keys = helpers.check_config_req(self.config)
                # Check config requirements
                if config_missing_keys:
                    print(".~" * 35)
                    print(
                        "Configuration file is missing the following keys. Some functionality may not work as expected."
                    )
                    print("Missing:", config_missing_keys)
                    print(".~" * 35)
            except FileNotFoundError:
                logger.error(
                    "Configuration file not found at path: %s", config_path
                )
                raise
            except ValueError as e:
                logger.error("Configuration file setup failed: %s", e)
                raise

        # extract info from config
        try:
            self.account_name = self.config["Batch"]["batch_account_name"]
            if not self.account_name:
                raise KeyError("Batch account name not found in config.")
            logger.debug("Batch account name loaded: %s", self.account_name)
        except Exception:
            logger.warning("Could not find batch account name in config.")

        try:
            self.resource_group_name = self.config["Authentication"][
                "resource_group"
            ]
        except Exception:
            logger.warning("Could not find resource group name in config.")

        # get credentials
        self._initialize_authentication(credential_method)
        logger.debug(f"generated credentials from {credential_method}.")
        self._initialize_registry()
        # create blob service account
        self.blob_service_client = "bsc"
        logger.debug("generated Blob Service Client.")
        # create batch mgmt client
        self.batch_mgmt_client = "bmc"
        logger.debug("generated Batch Management Client.")
        # Initialize storages
        if "Storage" in self.config.keys():
            self.storage_account_name = self.config["Storage"].get(
                "storage_account_name"
            )

        # create batch service client
        if "Batch" in self.config.keys():
            self.batch_client = "bsc"
            # Create pool
            self._initialize_pool()

        # Set up containers
        if "Container" in self.config.keys():
            self._initialize_containers()
        if self.pool_name and self.pool_parameters:
            self.create_pool(self.pool_name)
        logger.info("Client initialized! Happy coding!")

    def _initialize_authentication(self, credential_method):
        """Called by init method to set up authentication
        Args:
            config (str): config dict
        """
        if "credential_method" in self.config["Authentication"].keys():
            self.credential_method = credential_method = self.config[
                "Authentication"
            ]["credential_method"]
        else:
            self.credential_method = credential_method
        if "identity" in self.credential_method.lower():
            self.cred = "identity"
            logger.debug("ManagedIdentityCredential set.")
        elif "sp" in self.credential_method.lower():
            self.cred = "secret"
            logger.debug("ClientSecretCredential set.")
        elif "env" in self.credential_method.lower():
            keys = os.environ.keys()
            if (
                "AZURE_TENANT_ID" not in keys
                or "AZURE_CLIENT_ID" not in keys
                or "AZURE_CLIENT_SECRET" not in keys
            ):
                logger.error(
                    "Could not find AZURE_TENANT_ID, AZURE_CLIENT_ID or AZURE_CLIENT_SECRET environment variables."
                )
                raise Exception(
                    "Could not find AZURE_TENANT_ID, AZURE_CLIENT_ID or AZURE_CLIENT_SECRET environment variables."
                )
            else:
                self.cred = "environment"
                logger.debug("EnvironmentCredential set.")
        else:
            logger.error("No correct credential method provided.")
            raise Exception(
                "Please choose a credential_method from 'identity', 'sp', 'ext_user', 'env' and try again."
            )
        self.secret_cred = "secret"  # pragma: allowlist secret
        logger.debug("SecretCredential established.")
        self.batch_cred = "batch_cred"
        logger.debug("ServicePrincipalCredentials established.")

    def _initialize_registry(self):
        """Called by init to initialize the registry URL and details"""
        self.container_registry_server = None
        self.container_image_name = None
        self.full_container_name = None
        registry_name = None

        if "Container" in self.config.keys():
            if "registry_name" in self.config["Container"].keys():
                registry_name = self.config["Container"]["registry_name"]
                self.container_registry_server = f"{registry_name}.azurecr.io"
                self.registry_url = f"https://{self.container_registry_server}"
            else:
                self.registry_url = None

            if "repository_name" in self.config["Container"].keys():
                repository_name = self.config["Container"]["repository_name"]

            if "tag_name" in self.config["Container"].keys():
                tag_name = self.config["Container"]["tag_name"]
            else:
                tag_name = "latest"
        else:
            self.registry_url = None
            registry_name = None
            repository_name = None

        if registry_name and repository_name:
            self.set_azure_container(
                registry_name=registry_name,
                repo_name=repository_name,
                tag_name=tag_name,
            )
        logger.debug("Registry initialized.")

    def _initialize_pool(self):
        """Called by init to initialize the pool"""
        self.pool_parameters = None
        self.pool_name = (
            self.config["Batch"]["pool_name"]
            if "pool_name" in self.config["Batch"].keys()
            else None
        )
        self.scaling = (
            self.config["Batch"]["scaling_mode"]
            if "scaling_mode" in self.config["Batch"].keys()
            else None
        )
        if self.pool_name:
            if (
                self.scaling == "autoscale"
                and "autoscale_formula_path" in self.config["Batch"].keys()
            ):
                autoscale_formula_path = self.config["Batch"][
                    "autoscale_formula_path"
                ]
                print("Creating pool with autoscaling mode")
                self.set_pool_info(
                    mode=self.scaling,
                    autoscale_formula_path=autoscale_formula_path,
                )
            elif self.scaling == "fixed":
                dedicated_nodes = (
                    self.config["Batch"]["dedicated_nodes"]
                    if "dedicated_nodes" in self.config["Batch"].keys()
                    else 0
                )
                low_priority_nodes = (
                    self.config["Batch"]["low_priority_nodes"]
                    if "low_priority_nodes" in self.config["Batch"].keys()
                    else 1
                )
                self.set_pool_info(
                    mode=self.scaling,
                    dedicated_nodes=dedicated_nodes,
                    low_priority_nodes=low_priority_nodes,
                )
            else:
                pass
        else:
            pass
        logger.debug("Pool info initialized.")

    def _initialize_containers(self):
        """Called by init to initialize input and output containers"""
        self.input_container_name = (
            self.config["Container"]["input_container_name"]
            if "input_container_name" in self.config["Container"].keys()
            else None
        )
        self.output_container_name = (
            self.config["Container"]["output_container_name"]
            if "output_container_name" in self.config["Container"].keys()
            else None
        )
        # If we already have a Azure Batch pool, then mount the containers into pool
        if (
            self.input_container_name
            and self.output_container_name
            and self.pool_name
            and self.account_name
        ):
            self.update_containers(
                pool_name=self.pool_name,
                input_container_name=self.input_container_name,
                output_container_name=self.output_container_name,
                force_update=False,
            )
        logger.debug("Container info initialized.")

    def set_debugging(self, debug: bool) -> None:
        """required method that determines whether debugging is on or off. Debug = True for 'on', debug = False for 'off'.

        Args:
            debug (bool): True to turn debugging on, False to turn debugging off.
        """
        if debug is not True and debug is not False:
            logger.warning("Please use True or False to set debugging mode.")
        elif debug is True:
            self.debug = debug
            logger.info("You turned debugging on.")
            logger.info("This automatically disables autoscaling.")
            self.scaling = "fixed"
            logger.info("*" * 50)
            logger.info(
                "Jobs must be closed manually. Any jobs left running will continue to be billed as resources, which can rack up cloud costs."
            )
            logger.info("*" * 50)
        elif debug is False:
            self.debug = debug
            logger.debug("Debugging turned off.")

    def set_pool_info(
        self,
        mode: str,
        max_autoscale_nodes: int = 3,
        autoscale_formula_path: str = None,
        timeout=60,
        dedicated_nodes=0,
        low_priority_nodes=1,
        cache_blobfuse: bool = True,
        task_slots_per_node: int = 1,
        availability_zones: bool = False,
    ) -> None:
        """Sets the scaling mode of the client, either "fixed" or "autoscale".
        If "fixed" is selected, debug must be turned off.
        If "autoscale" is selected, an autoscale formula path must be provided.
        Other options include timeout, number of dedicated nodes and number of low-priority nodes.

        Args:
            mode (str): scaling mode for Batch. Either "fixed" or "autoscale".
            max_autoscale_nodes (int, optional): maximum number of nodes to scale up to for autoscaling pools. Used for default autoscale formula creation when no autoscale formula path provided.
            autoscale_formula_path (str, optional): path to autoscale formula file if mode is autoscale. Defaults to None.
            timeout (int, optional): length of time for tasks to run in pool. Defaults to 60.
            dedicated_nodes (int, optional): number of dedicated nodes for the pool. Defaults to 1.
            low_priority_nodes (int, optional): number of low priority nodes for the pool. Defaults to 0.
            cache_blobfuse (bool): True to use blobfuse caching, False to download data from blobfuse every time. Defaults to True.
            task_slots_per_node (int): number of task slots per node. Default 1.
            availability_zones (bool): whether to use availability zones for the pool. True to use Availability Zones. False to stay Regional. Default False.
        """
        # check if debug and scaling mode match, otherwise alert the user
        if self.debug is True and mode == "autoscale":
            logger.debug(
                "Debugging is set to True and autoscale is desired..."
            )
            logger.debug("This is not possible.")
            logger.info(
                "Either change debugging to False or set the scaling mode to fixed."
            )
            return None
        if mode == "autoscale" and autoscale_formula_path is None:
            use_default_autoscale_formula = True
            self.debug = False
            logger.debug(
                "Autoscale will be used with the default autoscale formula."
            )
        else:
            use_default_autoscale_formula = False
            logger.debug("Autoscale formula provided by user.")

        blob_config = []
        if self.mounts:
            for mount in self.mounts:
                blob_config.append(
                    helpers.get_blob_config(
                        mount[0], mount[1], cache_blobfuse, self.config
                    )
                )
                logger.debug(f"mount {mount} added to blob configuration.")

        self.mount_config = helpers.get_mount_config(blob_config)
        logger.debug("mount config saved to client.")
        if mode == "fixed" or mode == "autoscale":
            self.scaling = mode
            self.autoscale_formula_path = autoscale_formula_path
            self.timeout = timeout
            self.dedicated_nodes = dedicated_nodes
            self.low_priority_nodes = low_priority_nodes
            # create batch_json with fixed
            self.pool_parameters = helpers.get_pool_parameters(
                mode=mode,
                container_image_name=self.container_image_name,
                container_registry_url=self.registry_url,
                container_registry_server=self.container_registry_server,
                config=self.config,
                mount_config=self.mount_config,
                credential=self.secret_cred,
                autoscale_formula_path=autoscale_formula_path,
                timeout=timeout,
                dedicated_nodes=dedicated_nodes,
                low_priority_nodes=low_priority_nodes,
                use_default_autoscale_formula=use_default_autoscale_formula,
                max_autoscale_nodes=max_autoscale_nodes,
                task_slots_per_node=task_slots_per_node,
                availability_zones=availability_zones,
            )
            logger.debug("pool parameters generated")
        else:
            logger.warning("Please enter 'fixed' or 'autoscale' as the mode.")

    def create_blob_container(self, name: str, rel_mount_dir: str) -> None:
        """Creates an output container in Blob Storage.

        Args:
            name (str): desired name of output container.
            rel_mount_dir (str, optional): the path of the relative mount directory to use.
        """
        rel_mount_dir = helpers.format_rel_path(rel_mount_dir)
        # add to self.mounts
        self.mounts.append((name, rel_mount_dir))
        logger.debug(
            f"Mounted {name} with relative mount dir {rel_mount_dir}."
        )
        # create_container and save the container client
        mount_container_client = helpers.create_container(
            name, self.blob_service_client
        )
        self.mount_container_clients.append((name, mount_container_client))
        logger.debug(f"Created container client for container {name}.")

    def create_input_container(
        self, name: str, rel_mount_dir: str = "input"
    ) -> None:
        self.create_blob_container(name, rel_mount_dir)

    def create_output_container(
        self, name: str, rel_mount_dir: str = "output"
    ) -> None:
        self.create_blob_container(name, rel_mount_dir)

    def set_blob_container(self, name: str, rel_mount_dir: str) -> None:
        rel_mount_dir = helpers.format_rel_path(rel_mount_dir)
        logger.debug("formatted relative mount directory.")
        # check if container (folder) exists
        if not os.path.isdir(name):
            print("Folder does not exist.")
        self.mounts.append((name, rel_mount_dir))
        logger.debug(f"Added Blob container {name} to AzureClient.")

    def set_input_container(
        self, name: str, rel_mount_dir: str = "input"
    ) -> None:
        self.set_blob_container(name, rel_mount_dir)

    def set_output_container(
        self, name: str, rel_mount_dir: str = "output"
    ) -> None:
        self.set_blob_container(name, rel_mount_dir)

    def package_and_upload_dockerfile(
        self,
        registry_name: str,
        repo_name: str,
        tag: str,
        path_to_dockerfile: str = "./Dockerfile",
        use_device_code: bool = False,
    ) -> str:
        """package a docker container based on Dockerfile in repo and upload to specified location in Azure Container Registry

        Args:
            registry_name (str): name of registry in Azure CR
            repo_name (str): name of repo within ACR
            tag (str): tag for the uploaded docker container; ex: 'latest'
            path_to_dockerfile (str): path to Dockerfile. Default is path to Dockerfile in root of repo.
            use_device_code (bool): whether to use the flag --use_device_code for Azure CLI login. Default is False.

        Returns:
            str: full container name that was uploaded
        """
        self.full_container_name = helpers.package_and_upload_dockerfile(
            registry_name, repo_name, tag, path_to_dockerfile, use_device_code
        )
        logger.debug("Completed package_and_upload_dockerfile() function.")
        self.container_registry_server = f"{registry_name}.azurecr.io"
        self.registry_url = f"https://{self.container_registry_server}"
        self.container_image_name = f"https://{self.full_container_name}"
        return self.full_container_name

    def upload_docker_image(
        self,
        image_name: str,
        registry_name: str,
        repo_name: str,
        tag: str,
        use_device_code: bool = False,
    ) -> str:
        """Upload local docker image to Azure Container Registry.

        Args:
            image_name (str): local Docker image name
            registry_name (str): the name of the registry in Azure Container Registry
            repo_name (str): the name of the repo
            tag_name (str): the tag name
            use_device_code (bool): whether to use device code for authentication to ACR. Default False.

        Returns:
            str: full container name
        """
        self.full_container_name = helpers.upload_docker_image(
            image_name, registry_name, repo_name, tag, use_device_code
        )
        logger.debug("Completed package_and_upload_docker_image() function.")
        self.container_registry_server = f"{registry_name}.azurecr.io"
        self.registry_url = f"https://{self.container_registry_server}"
        self.container_image_name = f"https://{self.full_container_name}"
        return self.full_container_name

    def set_azure_container(
        self, registry_name: str, repo_name: str, tag_name: str
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
        container_name = helpers.check_azure_container_exists(
            registry_name, repo_name, tag_name, credential=self.cred
        )
        if container_name is not None:
            self.container_registry_server = f"{registry_name}.azurecr.io"
            self.registry_url = f"https://{self.container_registry_server}"
            self.container_image_name = f"https://{container_name}"
            self.full_container_name = container_name
            logger.debug("ACR container set.")
            return self.full_container_name
        else:
            logger.warning("ACR container does not exist.")
            return None

    def create_pool(self, pool_name: str) -> dict:
        """Creates the pool for Azure Batch jobs

        Args:
            pool_name (str): name of pool to create

        Raises:
            error: error raised if pool already exists by this name.

        Returns:
            dict: dictionary with pool name and creation time.
        """
        if self.pool_parameters is None:
            logger.exception(
                "No pool information given. Please use `set_pool_info()` before running `create_pool()`."
            )
            raise Exception(
                "No pool information given. Please use `set_pool_info()` before running `create_pool()`."
            ) from None

        start_time = datetime.datetime.now()
        self.pool_name = pool_name

        if self.scaling is None:
            # set scaling
            self.scaling = "autoscale"
            # set autoscale formula from default
        logger.info(
            f"Attempting to create a pool with {(self.config)['Batch']['pool_vm_size']} VMs."
        )
        logger.info(
            "Verify the size of the VM is appropriate for the use case."
        )

        # see if docker is running
        try:
            docker_env = docker.from_env(timeout=8)
            docker_env.ping()
        except Exception:
            print("can't find docker daemon")
            return None
        # check if image exists
        try:
            docker_env.images.get(self.full_container_name)
        except docker.errors.NotFound:
            print(
                f"image not found... make sure image {self.full_container_name} exists."
            )
            return None

        print("Verify the size of the VM is appropriate for the use case.")
        print("**Please use smaller VMs for dev/testing.**")
        try:
            self.pool = batch.Pool(
                pool_name, self.cont_name, self.pool_parameters
            )
            logger.info(f"Pool {pool_name!r} created.")
        except Exception:
            logger.warning(f"Pool {pool_name!r} already exists")

        # get mnt string
        mount_str = ""
        if self.mounts is not None:
            for mount in self.mounts:
                mount_str = (
                    mount_str
                    + "--mount type=bind,source="
                    + os.path.abspath(mount[0])
                    + f",target=/{mount[1]}"
                )
        # format pool info to save
        pool_info = {
            "image_name": self.full_container_name,
            "mount_str": mount_str,
        }
        # save pool info
        os.makedirs("tmp/pools", exist_ok=True)
        save_path = Path(f"tmp/pools/{pool_name}.txt")
        save_path.write_text(str(pool_info))

        end_time = datetime.datetime.now()
        return {
            "pool_id": pool_name,
            "creation_time": round((end_time - start_time).total_seconds(), 2),
        }

    def upload_files(
        self,
        files: list,
        container_name: str,
        location_in_blob: str = "",
        verbose: bool = False,
    ) -> None:
        """Uploads the files in the list to the input Blob storage container as stored in the client.

        Args:
            files (list): list of paths to files to upload
            container_name (str): name of Blob Storage Container to upload file to
            location_in_blob (str): the location (folder) inside the Blob container. Uploaded to root if "". Default is "".
            verbose (bool): whether to be verbose in uploaded files. Defaults to False
        """
        for file_name in files:
            helpers.upload_blob_file(
                filepath=file_name,
                location=location_in_blob,
                container_name=container_name,
                verbose=verbose,
            )
            logger.debug("Finished running helpers.upload_blob_file().")
        logger.debug("Uploaded all files in files list.")

    def upload_files_in_folder(
        self,
        folder_names: list[str],
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        location_in_blob: str = "",
        verbose: bool = True,
        force_upload: bool = True,
    ) -> list[str]:
        """Uploads all the files in folders provided

        Args:
            folder_names (list[str]): list of paths to folders
            container_name (str): the name of the Blob container
            include_extensions (str, list): a string or list of extensions desired for upload. Optional. Example: ".py" or [".py", ".csv"]
            exclude_extensions (str, list): a string or list of extensions of files not to include in the upload. Optional. Example: ".py" or [".py", ".csv"]
            location_in_blob (str): location (folder) to upload in Blob container. Will create the folder if it does not exist. Default is "" (root of Blob Container).
            verbose (bool): whether to print the name of files uploaded. Default True.
            force_upload (bool): whether to force the upload despite the file count in folder. Default False.

        Returns:
            list: list of all files uploaded
        """
        _files = []
        for _folder in folder_names:
            logger.debug(f"trying to upload folder {_folder}.")
            _uploaded_files = helpers.upload_files_in_folder(
                folder=_folder,
                container_name=container_name,
                include_extensions=include_extensions,
                exclude_extensions=exclude_extensions,
                location_in_blob=location_in_blob,
                verbose=verbose,
                force_upload=force_upload,
            )
            _files += _uploaded_files
        logger.debug(f"uploaded {_files}")
        self.files += _files
        return _files

    def set_pool(self, pool_name: str) -> None:
        """checks if pool exists and if it does, it gets assigned to the client

        Args:
            pool_name (str): name of pool
        """
        # check if pool exists
        fpath = Path(f"tmp/pools/{pool_name}.txt")
        if os.path.exists(fpath):
            # read in txt file
            pool_info = eval(fpath.read_text())
            self.full_container_name = pool_info["image_name"]
            # check if container exists
            try:
                docker_env = docker.from_env()
            except Exception:
                print("Could not ping docker to check pool existence")
                return None
            try:
                docker_env.images.get(self.full_container_name)
                self.image_name = self.full_container_name
                self.pool_name = pool_name
                self.pool = batch.Pool(pool_name, self.image_name)
            except docker.errors.NotFound:
                print(
                    f"image not found... make sure image {self.full_container_name} exists."
                )
                return None
        else:
            logger.warning(f"Pool {pool_name} does not exist.")
            logger.info("Choose an existing pool or create a new pool.")

    def add_job(
        self,
        job_id: str,
        pool_name: str | None = None,
        save_logs_to_blob: str | None = None,
        logs_folder: str | None = None,
        task_retries: int = 0,
        mark_complete_after_tasks_run: bool = False,
        task_id_ints: bool = False,
    ) -> None:
        """Adds a job to the pool and creates tasks based on input files.

        Args:
            job_id (str): name of job
            pool_name (str|None): pool to use for job. If None, will used self.pool_name from client. Default None.
            save_logs_to_blob (str): the name of the blob container. Must be mounted to the pool. Default None for no saving.
            logs_folder (str|None): the folder structure to use when saving logs to blob. Default None will save to /stdout_stderr/ folder in specified blob container.
            task_retries (int): number of times to retry a task that fails. Default 0.
            mark_complete_after_tasks_run (bool): whether to mark the job as completed when all tasks finish running. Default False.
            task_id_ints (bool): whether to use incremental integer values for task id values rather than a string. Default False.
        """
        # make sure the job_id does not have spaces
        job_id_r = job_id.replace(" ", "")
        logger.debug(f"job_id: {job_id_r}")

        if pool_name:
            p_name = pool_name
            self.pool_name = pool_name
        elif self.pool_name:
            p_name = self.pool_name
        else:
            logger.error("Please specify a pool for the job and try again.")
            raise Exception("Please specify a pool for the job and try again.")

        # check pool exists:
        if not os.path.exists(f"tmp/pools/{p_name}.txt"):
            print(f"Pool {p_name} does not exist.")
            return None
        self.save_logs_to_blob = save_logs_to_blob

        if logs_folder is None:
            self.logs_folder = "stdout_stderr"
        else:
            if logs_folder.startswith("/"):
                logs_folder = logs_folder[1:]
            if logs_folder.endswith("/"):
                logs_folder = logs_folder[:-1]
            self.logs_folder = logs_folder

        if task_id_ints:
            self.task_id_ints = True
        else:
            self.task_id_ints = False
        self.task_id_max = 0

        # add the job to the pool
        logger.debug(f"Attempting to add job {job_id_r}.")
        helpers.add_job(
            job_id=job_id_r,
            pool_id=p_name,
            task_retries=task_retries,
            mark_complete=mark_complete_after_tasks_run,
        )

        # check image for pool exists
        p_path = Path(f"tmp/pools/{p_name}.txt")
        pool_info = eval(p_path.read_text())
        image_name = pool_info["image_name"]
        mount_str = pool_info["mount_str"]
        # check if exists:
        dock_env = docker.from_env()
        try:
            d = dock_env.images.get(image_name)
            print(d.short_id)
        except Exception:
            print(f"Container {image_name} for pool could not be found.")

        # get mount string

        # get container name and run infinitely
        self.cont_name = (
            image_name.replace("/", "_").replace(":", "_") + f".{job_id_r}"
        )
        sp.run(
            f"docker run -d --rm {mount_str} --name {self.cont_name} {image_name} sleep inf",
            shell=True,
        )

        self.jobs.add(job_id_r)

    def delete_job(self, job_id: str):
        # delete the file
        job_id_r = job_id.replace(" ", "")
        os.remove(f"tmp/jobs/{job_id_r}.txt")

        # delete the container
        sp.run(f"docker stop {self.cont_name}", shell=True)

    def add_task(
        self,
        job_id: str,
        docker_cmd: list[str],
        name_suffix: str = "",
        depends_on: list[str] | None = None,
        depends_on_range: tuple | None = None,
        run_dependent_tasks_on_fail: bool = False,
        container: str = None,
    ) -> str:
        """adds task to existing job.

        Args:
            job_id (str): job id
            docker_cmd (list[str]): docker command to run
            name_suffix (str): suffix to add to task name for task identification. Default is an empty string.
            depends_on (list[str]): a list of tasks this task depends on. Default is None.
            depends_on_range (tuple): range of dependent tasks when task IDs are integers, given as (start_int, end_int). Optional.
            run_dependent_tasks_on_fail (bool): whether to run the dependent tasks if parent task fails. Default is False.
            container (str): name of ACR container in form "registry_name/repo_name:tag_name". Default is None to use container attached to client.

        Returns:
            str: task ID created
        """
        # run tasks for input files
        logger.debug("Adding task to job.")
        task_id = self.task_id_max
        print(f"Running {task_id}.")
        sp.run(f"""docker exec -i {self.cont_name} {docker_cmd}""", shell=True)

        self.task_id_max += 1
        return task_id

    def monitor_job(
        self,
        job_id: str,
        timeout: str | None = None,
        download_job_stats: bool = False,
    ) -> None:
        pass

    def run_dag(self, *args: Task, job_id: str, **kwargs):
        """
        Takes in tasks as arguments and runs them in the correct order as a DAG.

        Args:
            *args: batch.Task objects
            job_id (str): job name
            **kwargs: other keywords also accepted by client.add_task()

        Raises:
            ce: raises CycleError if submitted tasks do not form a DAG
        """
        # get topologicalsorter opject
        ts = TopologicalSorter()
        tasks = args
        for task in tasks:
            ts.add(task, *task.deps)
        try:
            task_order = [*ts.static_order()]
        except CycleError as ce:
            print("Submitted tasks do not form a DAG.")
            raise ce
        task_df = pd.DataFrame(columns=["id", "cmd", "deps"])
        # initialize df for task execution
        for i, task in enumerate(task_order):
            task_df.loc[i] = [task.id, task.cmd, task.deps]
        for task in task_order:
            tid = self.add_task(
                job_id=job_id,
                docker_cmd=task.cmd,
                depends_on=task_df[task_df["id"] == task.id]["deps"].values[0],
                **kwargs,
            )
            for i, dep in enumerate(task_df["deps"]):
                dlist = []
                for d in dep:
                    if str(d) == str(task.id):
                        dlist.append(tid)
                    else:
                        dlist.append(str(d))
                task_df.at[i, "deps"] = dlist
        return task_df

    def add_tasks_from_yaml(
        self, job_id: str, base_cmd: str, file_path: str, **kwargs
    ) -> list[str]:
        """
        parses yaml file to append parameters to a base command as command line arguments, which get submitted to the specified job.

        Args:
            job_id (str): name of job
            base_cmd (str): base command to which yaml parameters will be added
            file_path (str): path to yaml file

        Returns:
            list[str]: list of task IDs from submitted tasks
        """
        # get tasks from yaml
        task_strs = helpers.get_tasks_from_yaml(
            base_cmd=base_cmd, file_path=file_path
        )
        # submit tasks
        task_list = []
        for task_str in task_strs:
            tid = self.add_task(job_id=job_id, docker_cmd=task_str, **kwargs)
            task_list.append(tid)
        return task_list
