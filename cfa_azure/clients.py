import datetime
import json
import logging

from azure.core.exceptions import HttpResponseError

from cfa_azure import helpers

logger = logging.getLogger(__name__)


class AzureClient:
    def __init__(self, config_path: str):
        """Azure Client for interacting with Azure Batch, Container Registries and Blob Storage

        Args:
            config_path (str): path to configuration toml file
        """
        self.debug = None
        self.scaling = None
        self.input_container_name = None
        self.output_container_name = None
        self.files = []
        self.task_id_max = 0
        self.jobs = set()
        self.container_registry_server = None
        self.registry_url = None
        self.container_image_name = None
        self.full_container_name = None
        self.input_mount_dir = None
        self.output_mount_dir = None
        self.mounts = []
        self.mount_container_clients = []
        self.pool_parameters = None
        self.timeout = None

        logger.debug("Attributes initialized in client.")

        # load config
        self.config = helpers.read_config(config_path)
        logger.debug("config loaded")

        helpers.check_config_req(self.config)
        # extract info from config
        try:
            self.account_name = self.config["Batch"]["batch_account_name"]
            logger.debug("account name read in from config")
        except Exception as e:
            logger.warning("Batch account name not found in config.")
            logger.warning(
                "Please add the batch_account_name in the Batch section of the config."
            )
            raise e

        try:
            self.resource_group_name = self.config["Authentication"][
                "resource_group"
            ]
            logger.debug("resource group name read in from config")
        except Exception as e:
            logger.warning(e)
            raise e

        # get credentials
        self.sp_secret = helpers.get_sp_secret(self.config)
        logger.debug("generated SP secret.")
        self.sp_credential = helpers.get_sp_credential(self.config)
        logger.debug("generated SP credential.")
        # create blob service account
        self.blob_service_client = helpers.get_blob_service_client(self.config)
        logger.debug("generated Blob Service Client.")

        # create batch mgmt client
        self.batch_mgmt_client = helpers.get_batch_mgmt_client(self.config)
        logger.debug("generated Batch Management Client.")

        # create batch service client
        self.batch_client = helpers.get_batch_service_client(self.config)
        logger.info("Client initialized! Happy coding!")

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
        task_slots_per_node: int = 1
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
                mode = mode,
                container_image_name= self.container_image_name,
                container_registry_url=self.registry_url,
                container_registry_server=self.container_registry_server,
                config=self.config,
                mount_config=self.mount_config,
                autoscale_formula_path=autoscale_formula_path,
                timeout=timeout,
                dedicated_nodes=dedicated_nodes,
                low_priority_nodes=low_priority_nodes,
                use_default_autoscale_formula=use_default_autoscale_formula,
                max_autoscale_nodes=max_autoscale_nodes,
                task_slots_per_node= task_slots_per_node
            )
            logger.debug("pool parameters generated")
        else:
            logger.warning("Please enter 'fixed' or 'autoscale' as the mode.")

    def create_input_container(
        self, name: str, input_mount_dir: str = "input"
    ) -> None:
        """Creates an input container in Blob Storage.

        Args:
            name (str): desired name of input container.
            input_mount_dir (str, optional): the path of the input mount directory. Defaults to "input".
        """
        self.input_container_name = name
        self.input_mount_dir = helpers.format_rel_path(input_mount_dir)
        # add to self.mounts
        self.mounts.append((name, self.input_mount_dir))
        logger.debug(
            f"Mounted {name} with relative mount dir {self.input_mount_dir}."
        )
        # create container and save the container client
        self.in_cont_client = helpers.create_container(
            self.input_container_name, self.blob_service_client
        )
        logger.debug("Created container client for input container.")

    def create_output_container(
        self, name: str, output_mount_dir: str = "output"
    ) -> None:
        """Creates an output container in Blob Storage.

        Args:
            name (str): desired name of output container.
            output_mount_dir (str, optional): the path of the output mount directory. Defaults to "output".
        """
        self.output_container_name = name
        self.output_mount_dir = helpers.format_rel_path(output_mount_dir)
        # add to self.mounts
        self.mounts.append((name, self.output_mount_dir))
        logger.debug(
            f"Mounted {name} with relative mount dir {self.output_mount_dir}."
        )
        # create_container and save the container client
        self.out_cont_client = helpers.create_container(
            self.output_container_name, self.blob_service_client
        )
        logger.debug("Created container client for output container.")

    def create_blob_container(self, name: str, rel_mount_dir: str) -> None:
        """Creates an output container in Blob Storage.

        Args:
            name (str): desired name of output container.
            output_mount_dir (str, optional): the path of the output mount directory. Defaults to "output".
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

    def set_input_container(
        self, name: str, input_mount_dir: str = "input"
    ) -> None:
        """Sets the input container to be used with the client.

        Args:
            name (str): name of input container
            input_mount_dir (str, optional): input mount directory. Defaults to "input".
        """
        container_client = self.blob_service_client.get_container_client(
            container=name
        )
        logger.debug(
            "input container client generated from blob service client."
        )
        input_mount_dir = helpers.format_rel_path(input_mount_dir)
        logger.debug("formatted relative mount directory.")
        if not container_client.exists():
            logger.warning(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.input_container_name = name
            self.input_mount_dir = input_mount_dir
            self.in_cont_client = container_client
            self.mounts.append((name, input_mount_dir))
            logger.debug(f"Added input Blob container {name} to AzureClient.")

    def set_output_container(
        self, name: str, output_mount_dir: str = "output"
    ) -> None:
        """Sets the output container to be used with the client.

        Args:
            name (str): name of output container
            output_mount_dir (str, optional): output mount directory. Defaults to "output".
        """
        output_mount_dir = helpers.format_rel_path(output_mount_dir)
        logger.debug("formatted relative mount directory.")
        container_client = self.blob_service_client.get_container_client(
            container=name
        )
        logger.debug(
            "output container client generated from blob service client."
        )
        if not container_client.exists():
            logger.warning(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.output_container_name = name
            self.output_mount_dir = output_mount_dir
            self.out_cont_client = container_client
            self.mounts.append((name, output_mount_dir))
            logger.debug(f"Added output Blob container {name} to AzureClient.")

    def set_blob_container(self, name: str, rel_mount_dir: str) -> None:
        """Sets the output container to be used with the client.

        Args:
            name (str): name of output container
            rel_mount_dir (str, optional): relative mount directory.
        """
        rel_mount_dir = helpers.format_rel_path(rel_mount_dir)
        logger.debug("formatted relative mount directory.")
        container_client = self.blob_service_client.get_container_client(
            container=name
        )
        logger.debug(
            "Blob container client generated from blob service client."
        )
        if not container_client.exists():
            logger.warning(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.mounts.append((name, rel_mount_dir))
            logger.debug(f"Added Blob container {name} to AzureClient.")


    def update_containers(
            self,
            input_container_name:str,
            output_container_name:str,
            pool_name:str=None,
            force_update:bool=False
    ) -> str | None:
        """Changes the input and/or output containers mounted in an existing Azure batch pool

        Args:
            pool_name (str|None): pool to use for job. If None, will used self.pool_name from client. Default None.
            input_container_name (str): unique identifier for the Blob storage container that will be mapped to /input path
            output_container_name (str): unique identifier for the Blob storage container that will be mapped to /output path
            force_update (bool): optional, deletes the existing pool without checking if it is already running any tasks 
        """
        # Check if pool already exists
        if not pool_name:
            pool_name = self.pool_name
        # Check if pool already exists
        if helpers.check_pool_exists(self.resource_group_name, self.account_name, pool_name, self.batch_mgmt_client):
            if not force_update:
                # Check how many jobs are currently running in pool
                active_nodes = list(helpers.list_nodes_by_pool(pool_name=pool_name, config=self.config, node_state='running'))
                if len(active_nodes) > 0:
                    logger.error(f"There are {len(active_nodes)} compute nodes actively running tasks in pool {pool_name}. Please wait for jobs to complete or retry withy force_update=True.")
                    return None

            container_image_name = self.get_container_image_name(pool_name)

            # Delete existing pool
            logger.info(f"Deleting pool {pool_name}")
            helpers.delete_pool(
                resource_group_name=self.resource_group_name,
                account_name=self.account_name,
                pool_name=pool_name,
                batch_mgmt_client=self.batch_mgmt_client,
            )
        else:
            logger.info(f"Pool {pool_name} does not exist. New pool will be created.")
            container_image_name = self.config["Container"]["container_image_name"]

        if not 'pool_id' in self.config['Batch']:
            self.config['Batch']['pool_id'] = pool_name

        # Recreate the pool
        mount_config = [
            {
                "azureBlobFileSystemConfiguration": {
                    "accountName": self.config["Storage"]["storage_account_name"],
                    "identityReference": {
                        "resourceId": self.config["Authentication"][
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
                    "accountName": self.config["Storage"]["storage_account_name"],
                    "identityReference": {
                        "resourceId": self.config["Authentication"][
                            "user_assigned_identity"
                        ]
                    },
                    "containerName": output_container_name,
                    "blobfuseOptions": "",
                    "relativeMountPath": "output",
                }
            }
        ]
        pool_parameters = helpers.get_pool_parameters(
            mode=self.scaling,
            container_image_name=container_image_name,
            container_registry_url=self.registry_url,
            container_registry_server=self.container_registry_server,
            config=self.config,
            mount_config=mount_config
        )
        batch_json = {
            'pool_id': pool_name,
            'pool_parameters': pool_parameters
        }
        pool_name = helpers.create_batch_pool(batch_mgmt_client=self.batch_mgmt_client, batch_json=batch_json)
        return pool_name


    def update_scale_settings(
        self,
        scaling:str,
        pool_name:str=None,
        dedicated_nodes:int=None,
        low_priority_nodes:int=None,
        node_deallocation_option:int=None,
        autoscale_formula_path:str=None,
        evaluation_interval:str=None
    ) -> dict | None:
        """Updates scale mode (fixed or autoscale) and related settings for an existing Azure batch pool

        Args:
            pool_name (str|None): pool to use for job. If None, will used self.pool_name from client. Default None.
            dedicated_nodes (int): optional, the target number of dedicated compute nodes for the pool in fixed scaling mode. Defaults to None.
            low_priority_nodes (int): optional, the target number of spot compute nodes for the pool in fixed scaling mode. Defaults to None.
            node_deallocation_option (str): optional, determines what to do with a node and its running tasks after it has been selected for deallocation. Defaults to None.
            autoscale_formula_path (str): optional, path to autoscale formula file if mode is autoscale. Defaults to None.
            evaluation_interval (str): optional, how often Batch service should adjust pool size according to its autoscale formula. Defaults to 15 minutes. 
        """
        if pool_name:
            p_name = pool_name
        elif self.pool_name:
            p_name = self.pool_name
        else:
            logger.error("Please specify a pool and try again.")
            raise Exception("Please specify a pool and try again.") from None
        scale_settings = {}
        self.scaling = scaling
        if scaling == "autoscale":
            # Autoscaling configuration
            validation_errors = helpers.check_autoscale_parameters(mode=scaling, dedicated_nodes=dedicated_nodes, low_priority_nodes=low_priority_nodes, node_deallocation_option=node_deallocation_option)
            if validation_errors:
                logger.error(validation_errors)
                raise Exception(validation_errors) from None
            autoScalingParameters = {}
            if autoscale_formula_path:
                self.autoscale_formula_path = autoscale_formula_path
                formula = helpers.get_autoscale_formula(filepath=autoscale_formula_path)
                if formula:
                    autoScalingParameters['formula'] = formula
            if evaluation_interval:
                autoScalingParameters['evaluationInterval'] = evaluation_interval
            scale_settings['autoScale'] = autoScalingParameters
        else:
            validation_errors = helpers.check_autoscale_parameters(mode=scaling, autoscale_formula_path=autoscale_formula_path, evaluation_interval=evaluation_interval)
            if validation_errors:
                logger.error(validation_errors)
                raise Exception(validation_errors) from None
            # Fixed scaling
            fixedScalingParameters = {}
            if dedicated_nodes:
                fixedScalingParameters["targetDedicatedNodes"] = dedicated_nodes
            if low_priority_nodes:
                fixedScalingParameters["targetLowPriorityNodes"] = low_priority_nodes
            if node_deallocation_option:
                fixedScalingParameters["nodeDeallocationOption"] = node_deallocation_option
            scale_settings['fixedScale'] = fixedScalingParameters

        if scale_settings:
            pool_parameters = {
                "properties": {
                    "scaleSettings": scale_settings
                }
            }
            return helpers.update_pool(pool_name = p_name, 
                                       pool_parameters = pool_parameters, 
                                       batch_mgmt_client = self.batch_mgmt_client, 
                                       account_name = self.account_name, 
                                       resource_group_name = self.resource_group_name)


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
            raise Exception("No pool information given. Please use `set_pool_info()` before running `create_pool()`.") from None

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
        print("Verify the size of the VM is appropriate for the use case.")
        print("**Please use smaller VMs for dev/testing.**")
        try:
            self.batch_mgmt_client.pool.create(
                resource_group_name=self.resource_group_name,
                account_name=self.account_name,
                pool_name=self.pool_name,
                parameters=self.pool_parameters,
            )
            logger.info(f"Pool {pool_name!r} created.")
        except HttpResponseError as error:
            if "PropertyCannotBeUpdated" in error.message:
                logger.warning(f"Pool {pool_name!r} already exists")
            else:
                raise error

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
            location (str): the location (folder) inside the Blob container. Uploaded to root if "". Default is "".
            verbose (bool): whether to be verbose in uploaded files. Defaults to False
        """
        container_client = self.blob_service_client.get_container_client(
            container=container_name
        )
        logger.debug(f"Container client generated for {container_name}.")
        if not container_client.exists():
            logger.error(
                f"Blob container {container_name} does not exist. Please try again with an existing Blob container."
            )
            raise Exception(f"Blob container {container_name} does not exist. Please try again with an existing Blob container.") from None

        for file_name in files:
            helpers.upload_blob_file(
                filepath=file_name,
                location=location_in_blob,
                container_client=container_client,
                verbose=verbose,
            )
            logger.debug("Finished running helpers.upload_blob_file().")
        logger.debug("Uploaded all files in files list.")

    def upload_files_in_folder(
        self,
        folder_names: list[str],
        container_name: str,
        include_extensions: str|list|None = None,
        exclude_extensions: str|list|None = None,
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
                blob_service_client=self.blob_service_client,
                verbose=verbose,
                force_upload=force_upload,
            )
            _files += _uploaded_files
        logger.debug(f"uploaded {_files}")
        self.files += _files
        return _files

    def add_job(
        self,
        job_id: str,
        pool_name: str | None = None,
        end_job_on_task_failure: bool = False,
        task_retries: int = 3
    ) -> None:
        """Adds a job to the pool and creates tasks based on input files.

        Args:
            job_id (str): name of job
            pool_name (str|None): pool to use for job. If None, will used self.pool_name from client. Default None.
            end_job_on_task_failure (bool): whether to end the job if a task fails. Default False.
            task_retries (int): the maximum number of retries for a task that fails. Default 3 retries.
        """
        # make sure the job_id does not have spaces
        job_id_r = job_id.replace(" ", "")
        logger.debug(f"job_id: {job_id_r}")

        if pool_name:
            p_name = pool_name
        elif self.pool_name:
            p_name = self.pool_name
        else:
            logger.error("Please specify a pool for the job and try again.")
            raise Exception("Please specify a pool for the job and try again.")
        # add the job to the pool
        logger.debug(f"Attempting to add job {job_id_r}.")
        helpers.add_job(
            job_id=job_id_r,
            pool_id=p_name,
            end_job_on_task_failure=end_job_on_task_failure,
            batch_client=self.batch_client,
            task_retries=task_retries
        )
        self.jobs.add(job_id_r)

    def add_task(
        self,
        job_id: str,
        docker_cmd: list[str],
        name_suffix: str = "",
        use_uploaded_files: bool = False,
        input_files: list[str] | None = None,
        depends_on: list[str] | None = None,
        container: str = None,
    ) -> list[str]:
        """adds task to existing job.
        If files have been uploaded, the docker command will be applied to each file.
        If input files are specified, the docker command will be applied to only those files.
        If no input files are specified, only the docker command will be run.

        Args:
            job_id (str): job id
            docker_cmd (list[str]): docker command to run
            name_suffix (str): suffix to add to task name for task identification. Default is an empty string.
            use_uploaded_files (bool): whether to use uploaded files with the docker command. This will append the docker command with the names of the input files
                and create a task for each input file uploaded or specified in input_files. Default is False.
            input_files (list[str]): a list of file names. Each file will be assigned its own task and executed against the docker command provided. Default is [].
            depends_on (list[str]): a list of tasks this task depends on. Default is None.
            container (str): name of ACR container in form "registry_name/repo_name:tag_name". Default is None to use container attached to client.


        Returns:
            list: list of task IDs created
        """
        if use_uploaded_files:
            if input_files:
                in_files = input_files
            elif self.files:
                in_files = self.files
            else:
                logger.warning(
                    "use_uploaded_files set to True but no input files found."
                )
        else:
            in_files = None

        if container is not None:
            # check container exists
            logger.debug("Checking the container exists.")
            registry = container.split("/")[0]
            repo_tag = container.split("/")[-1]
            repo = repo_tag.split(":")[0]
            tag = repo_tag.split(":")[-1]
            container_name = helpers.check_azure_container_exists(
                registry, repo, tag
            )
            if container_name is None:
                raise ValueError(f"{container} does not exist.")
        else:
            if self.full_container_name is None:
                logger.debug("Gettting full pool info")
                pool_info = helpers.get_pool_full_info(
                    self.resource_group_name,
                    self.account_name,
                    self.pool_name,
                    self.batch_mgmt_client,
                )
                logger.debug("Generated full pool info.")
                vm_config = (
                    pool_info.deployment_configuration.virtual_machine_configuration
                )
                logger.debug("Generated VM config.")
                pool_container = (
                    vm_config.container_configuration.container_image_names
                )
                container_name = pool_container[0].split("://")[-1]
                logger.debug(f"Container name set to {container_name}.")
            else:
                container_name = self.full_container_name
                logger.debug(f"Container name set to {container_name}.")

        # run tasks for input files
        logger.debug("Adding tasks to job.")
        task_ids = helpers.add_task_to_job(
            job_id=job_id,
            task_id_base=job_id,
            docker_command=docker_cmd,
            name_suffix = name_suffix,
            input_files=in_files,
            mounts=self.mounts,
            depends_on=depends_on,
            batch_client=self.batch_client,
            full_container_name=container_name,
            task_id_max=self.task_id_max,
        )
        self.task_id_max += 1
        return task_ids

    def monitor_job(self, job_id: str, timeout: str | None = None) -> None:
        """monitor the tasks running in a job

        Args:
            job_id (str): job id
        """
        # monitor the tasks
        logger.debug(f"starting to monitor job {job_id}.")
        monitor = helpers.monitor_tasks(
            job_id,
            timeout,
            self.batch_client,
            self.resource_group_name,
            self.account_name,
            self.pool_name,
            self.batch_mgmt_client,
        )
        print(monitor)

        # delete job automatically if debug is false
        if self.debug is False:
            logger.info("Cleaning up - deleting job")
            # Delete job
            self.batch_client.job.delete(job_id)
        elif self.debug is True:
            logger.info("Job complete. Time to debug. Job not deleted.")
            logger.info("**Remember to close out the job when debugging.**")

    def check_job_status(self, job_id: str) -> None:
        """checks various components of a job
        - whether job exists
        - prints number of completed tasks
        - prints the state of a job: completed, activate, etc.

        Args:
            job_id (str): name of job
        """
        # whether job exists
        logger.debug("Checking job exists.")
        if helpers.check_job_exists(job_id, self.batch_client):
            logger.debug(f"Job {job_id} exists.")
            c_tasks = helpers.get_completed_tasks(job_id, self.batch_client)
            logger.info("Task info:")
            logger.info(c_tasks)
            if helpers.check_job_complete(job_id, self.batch_client):
                logger.info(f"Job {job_id} completed.")
            else:
                j_state = helpers.get_job_state(job_id, self.batch_client)
                logger.info(f"Job in {j_state} state")
        else:
            logger.info(f"Job {job_id} does not exist.")

    def delete_job(self, job_id: str) -> None:
        """delete a specified job

        Args:
            job_id (str): job id of job to terminate and delete
        """
        logger.debug(f"Attempting to delete {job_id}.")
        self.batch_client.job.delete(job_id)
        logger.info(f"Job {job_id} deleted.")

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
            registry_name, repo_name, tag_name
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

    def download_file(
        self,
        src_path: str,
        dest_path: str,
        container_name: str = None,
        do_check: bool = True
    ) -> None:
        """download a file from Blob storage

        Args:
            src_path (str):
                Path within the container to the desired file (including filename)
            dest_path (str):
                Path to desired location to save the downloaded file
            container_name (str):
                Name of the storage container containing the file to be downloaded.
            do_check (bool):
                Whether or not to do an existence check\
        """
        # use the output container client by default for downloading files
        logger.debug(f"Creating container client for {container_name}.")
        c_client = self.blob_service_client.get_container_client(container = container_name)
        
        logger.debug("Attempting to download file.")
        helpers.download_file(
            c_client, src_path, dest_path, do_check
        )

    def download_directory(
        self, src_path: str, dest_path: str, container_name: str,
        include_extensions: str|list|None = None,
        exclude_extensions: str|list|None = None,
        verbose = True
    ) -> None:
        """download a whole directory from Azure Blob Storage

        Args:
        src_path (str):
            Prefix of the blobs to download
        dest_path (str):
            Path to the directory in which to store the downloads
        container_name (str):
            name of Blob container
        include_extensions (str, list, None):
            a string or list of extensions to include in the download. Optional.
        exclude_extensions (str, list, None):
            a string of list of extensions to exclude from the download. Optional.
        verbose (bool):
            a Boolean whether to print file names when downloaded.
        """
        logger.debug("Attempting to download directory.")
        helpers.download_directory(
            container_name, src_path, dest_path,
            self.blob_service_client,
            include_extensions,
            exclude_extensions,
            verbose
        )
        logger.debug("finished call to download")

        
    def set_pool(self, pool_name: str) -> None:
        """checks if pool exists and if it does, it gets assigned to the client

        Args:
            pool_name (str): name of pool
        """
        # check if pool exists
        if helpers.check_pool_exists(
            self.resource_group_name,
            self.account_name,
            pool_name,
            self.batch_mgmt_client,
        ):
            self.pool_name = pool_name
            _info = helpers.get_pool_info(
                self.resource_group_name,
                self.account_name,
                pool_name,
                self.batch_mgmt_client,
            )
            vm_size = str(json.loads(_info)["vm_size"])
            logger.info(f"Pool {pool_name} uses {vm_size} VMs.")
            logger.info("Make sure the VM size matches the use case.\n")
        else:
            logger.warning(f"Pool {pool_name} does not exist.")
            logger.info("Choose an existing pool or create a new pool.")

    def get_pool_info(self) -> dict:
        """Retrieve information about pool used by client.

        Returns:
            dict: dictionary of pool information
        """
        pool_info = helpers.get_pool_info(
            self.resource_group_name,
            self.account_name,
            self.pool_name,
            self.batch_mgmt_client,
        )
        return pool_info

    def get_pool_full_info(self, pool_name:str=None) -> dict:
        """Retrieve full information about pool used by client.

        Returns:
        - dict: instance of batch_mgmt_client.pool.get()

        """
        if not pool_name:
            pool_name = self.pool_name
        pool_info = helpers.get_pool_full_info(
            self.resource_group_name,
            self.account_name,
            pool_name,
            self.batch_mgmt_client,
        )
        return pool_info

    def get_container_image_name(self, pool_name:str) -> str:
        pool_info = self.get_pool_full_info(pool_name)
        vm_config = (pool_info.deployment_configuration.virtual_machine_configuration)
        pool_container = (vm_config.container_configuration.container_image_names)
        container_image_name = pool_container[0].split("://")[-1]
        return container_image_name

    def delete_pool(self, pool_name: str) -> None:
        """Delete the specified pool from Azure Batch.

        Args:
            pool_name (str): name of Batch Pool to delete
        """
        helpers.delete_pool(
            resource_group_name=self.resource_group_name,
            account_name=self.account_name,
            pool_name=pool_name,
            batch_mgmt_client=self.batch_mgmt_client,
        )

    def list_blob_files(self, blob_container: str = None):
        if not self.mounts and blob_container is None:
            logger.warning(
                "Please specify a blob container or have mounts associated with the client."
            )
            return None
        if blob_container:
            logger.debug(f"Listing blobs in {blob_container}")
            filenames = helpers.list_blobs_flat(
                container_name=blob_container,
                blob_service_client=self.blob_service_client,
                verbose=False,
            )
        elif self.mounts:
            logger.debug("Looping through mounts.")
            filenames = []
            for mount in self.mounts:
                _files = helpers.list_blobs_flat(
                    container_name=mount[0],
                    blob_service_client=self.blob_service_client,
                    verbose=False,
                )
                filenames += _files
        return filenames

    def delete_blob_file(self, blob_name, container_name):
        logger.debug(f"Deleting blob {blob_name} from {container_name}.")
        helpers.delete_blob_snapshots(
            blob_name, container_name, self.blob_service_client
        )
        logger.debug(f"Deleted {blob_name}.")

    def delete_blob_folder(self, folder_path, container_name):
        logger.debug(f"Deleting files in {folder_path} folder.")
        helpers.delete_blob_folder(
            folder_path, container_name, self.blob_service_client
        )
        logger.debug(f"Deleted folder {folder_path}.")

    def mark_job_completed_after_tasks_run(self,
        job_id: str, mark_complete: bool = True,
        ):
        helpers.mark_job_completed_after_tasks_run(
        job_id = job_id,
        pool_id = self.pool_name,
        batch_client = self.batch_client,
        mark_complete = mark_complete)
