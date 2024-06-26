import datetime
import json

from azure.core.exceptions import HttpResponseError

from cfa_azure import batch, helpers


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

        # load config
        self.config = helpers.read_config(config_path)

        helpers.check_config_req(self.config)
        # extract info from config
        try:
            self.account_name = self.config["Batch"]["batch_account_name"]
        except Exception:
            print("Batch account name not found in config.")
            print(
                "Please add the batch_account_name in the Batch section of the config."
            )

        try:
            self.resource_group_name = self.config["Authentication"][
                "resource_group"
            ]
        except Exception as e:
            print(e)

        # get credentials
        self.sp_secret = helpers.get_sp_secret(self.config)
        self.sp_credential = helpers.get_sp_credential(self.config)

        # create blob service account
        self.blob_service_client = helpers.get_blob_service_client(self.config)

        # create batch mgmt client
        self.batch_mgmt_client = helpers.get_batch_mgmt_client(self.config)

        # create batch service client
        self.batch_client = helpers.get_batch_service_client(self.config)
        print("Client initialized! Happy coding!")

    def set_debugging(self, debug: bool) -> None:
        """required method that determines whether debugging is on or off. Debug = True for 'on', debug = False for 'off'.

        Args:
            debug (bool): True to turn debugging on, False to turn debugging off.
        """
        if debug is not True and debug is not False:
            print("Please use True or False to set debugging mode.")
        elif debug is True:
            self.debug = debug
            print("You turned debugging on.")
            print("This automatically disables autoscaling.")
            self.scaling = "fixed"
            print("*" * 50)
            print(
                "Jobs must be closed manually. Any jobs left running will continue to be billed as resources, which can rack up cloud costs."
            )
            print("*" * 50)
        elif debug is False:
            self.debug = debug

    def set_pool_info(
        self,
        mode: str,
        max_autoscale_nodes: int = 3,
        autoscale_formula_path: str = None,
        timeout=60,
        dedicated_nodes=1,
        low_priority_nodes=0,
        cache_blobfuse: bool = True,
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
        """
        # check if debug and scaling mode match, otherwise alert the user
        if self.debug is True and mode == "autoscale":
            print("Debugging is set to True and autoscale is desired...")
            print("This is not possible.")
            print(
                "Either change debugging to False or set the scaling mode to fixed."
            )
            return None
        if mode == "autoscale" and autoscale_formula_path is None:
            use_default_autoscale_formula = True
            self.debug = False
        else:
            use_default_autoscale_formula = False

        blob_config = []
        if self.mounts:
            for mount in self.mounts:
                blob_config.append(
                    helpers.get_blob_config(
                        mount[0], mount[1], cache_blobfuse, self.config
                    )
                )

        self.mount_config = helpers.get_mount_config(blob_config)
        if mode == "fixed" or mode == "autoscale":
            self.scaling = mode
            self.autoscale_formula_path = autoscale_formula_path
            self.timeout = timeout
            self.dedicated_nodes = dedicated_nodes
            self.low_priority_nodes = low_priority_nodes
            # create batch_json with fixed
            self.pool_parameters = helpers.get_pool_parameters(
                mode,
                self.container_image_name,
                self.registry_url,
                self.container_registry_server,
                self.config,
                self.mount_config,
                autoscale_formula_path,
                timeout,
                dedicated_nodes,
                low_priority_nodes,
                use_default_autoscale_formula,
                max_autoscale_nodes,
            )
        else:
            print("Please enter 'fixed' or 'autoscale' as the mode.")

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
        # create container and save the container client
        self.in_cont_client = helpers.create_container(
            self.input_container_name, self.blob_service_client
        )

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
        # create_container and save the container client
        self.out_cont_client = helpers.create_container(
            self.output_container_name, self.blob_service_client
        )

    def create_blob_container(self, name: str, rel_mount_dir: str) -> None:
        """Creates an output container in Blob Storage.

        Args:
            name (str): desired name of output container.
            output_mount_dir (str, optional): the path of the output mount directory. Defaults to "output".
        """
        rel_mount_dir = helpers.format_rel_path(rel_mount_dir)
        # add to self.mounts
        self.mounts.append((name, rel_mount_dir))
        # create_container and save the container client
        mount_container_client = helpers.create_container(
            name, self.blob_service_client
        )
        self.mount_container_clients.append((name, mount_container_client))

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
        input_mount_dir = helpers.format_rel_path(input_mount_dir)
        if not container_client.exists():
            print(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.input_container_name = name
            self.input_mount_dir = input_mount_dir
            self.in_cont_client = container_client
            self.mounts.append((name, input_mount_dir))

    def set_output_container(
        self, name: str, output_mount_dir: str = "output"
    ) -> None:
        """Sets the output container to be used with the client.

        Args:
            name (str): name of output container
            output_mount_dir (str, optional): output mount directory. Defaults to "output".
        """
        output_mount_dir = helpers.format_rel_path(output_mount_dir)
        container_client = self.blob_service_client.get_container_client(
            container=name
        )
        if not container_client.exists():
            print(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.output_container_name = name
            self.output_mount_dir = output_mount_dir
            self.out_cont_client = container_client
            self.mounts.append((name, output_mount_dir))

    def set_blob_container(self, name: str, rel_mount_dir: str) -> None:
        """Sets the output container to be used with the client.

        Args:
            name (str): name of output container
            rel_mount_dir (str, optional): relative mount directory.
        """
        rel_mount_dir = helpers.format_rel_path(rel_mount_dir)
        container_client = self.blob_service_client.get_container_client(
            container=name
        )
        if not container_client.exists():
            print(
                f"Container [{name}] does not exist. Please create it if desired."
            )
        else:
            self.mounts.append((name, rel_mount_dir))

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
            raise Exception(
                "No pool information given. Please use `set_pool_info()` before running `create_pool()`."
            )

        start_time = datetime.datetime.now()
        self.pool_name = pool_name

        if self.scaling is None:
            # set scaling
            self.scaling = "autoscale"
            # set autoscale formula from default
        print(
            f"Attempting to create a pool with {(self.config)['Batch']['pool_vm_size']} VMs."
        )
        print("Verify the size of the VM is appropriate for the use case.")
        try:
            self.batch_mgmt_client.pool.create(
                resource_group_name=self.resource_group_name,
                account_name=self.account_name,
                pool_name=self.pool_name,
                parameters=self.pool_parameters,
            )
            print(f"Pool {pool_name!r} created.")
        except HttpResponseError as error:
            if "PropertyCannotBeUpdated" in error.message:
                print(f"Pool {pool_name!r} already exists")
            else:
                raise error

        end_time = datetime.datetime.now()
        return {
            "pool_id": pool_name,
            "creation_time": round((end_time - start_time).total_seconds(), 2),
        }

    def upload_files(self, files: list, blob_container: str = None) -> None:
        """Uploads the files in the list to the input Blob storage container as stored in the client.

        Args:
            files (list): list of paths to files to upload
            blob_container (str): name of Blob container for file upload. Defaults to None.
        """
        if blob_container is None:
            blob_c_name = self.input_container_name
        else:
            blob_c_name = blob_container

        for file_name in files:
            shortname = file_name.split("/")[-1]
            blob_client = self.blob_service_client.get_blob_client(
                container=blob_c_name, blob=shortname
            )
            with open(file_name, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"Uploaded {file_name!r} to container {blob_c_name}.")
            self.files.append(shortname)

    def upload_files_in_folder(
        self,
        folder_names: list[str],
        blob_container: str = None,
        verbose: bool = False,
        force_upload: bool = False,
    ) -> list[str]:
        """Uploads all the files in folders provided

        Args:
            folder_names (list[str]): list of paths to folders
            blob_container (str): name of Blob container for file upload. Defaults to None.
            verbose (bool): whether to print the name of files uploaded. Default False.
            force_upload (bool): whether to force the upload despite the file count in folder. Default False.

        Returns:
            list: list of all files uploaded
        """
        if blob_container is None:
            blob_c_name = self.input_container_name
        else:
            blob_c_name = blob_container

        _files = batch.upload_files_to_container(
            folder_names,
            blob_c_name,
            self.blob_service_client,
            verbose,
            force_upload,
        )
        print(f"uploaded {_files}")
        self.files += _files
        return _files

    def add_job(
        self, job_id: str, end_job_on_task_failure: bool = False
    ) -> None:
        """Adds a job to the pool and creates tasks based on input files.

        Args:
            job_id (str): name of job
            end_job_on_task_failure (bool): whether to end the job if a task fails. Default False.
        """
        # make sure the job_id does not have spaces
        job_id_r = job_id.replace(" ", "")
        print(f"job_id: {job_id_r}")

        # add the job to the pool
        helpers.add_job(
            job_id=job_id_r,
            pool_id=self.pool_name,
            end_job_on_task_failure=end_job_on_task_failure,
            batch_client=self.batch_client,
        )
        self.jobs.add(job_id_r)

    def add_task(
        self,
        job_id: str,
        docker_cmd: list[str],
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
                print(
                    "use_uploaded_files set to True but no input files found."
                )
        else:
            in_files = None

        if container is not None:
            # check container exists
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
                pool_info = helpers.get_pool_full_info(
                    self.resource_group_name,
                    self.account_name,
                    self.pool_name,
                    self.batch_mgmt_client,
                )
                vm_config = (
                    pool_info.deployment_configuration.virtual_machine_configuration
                )
                pool_container = (
                    vm_config.container_configuration.container_image_names
                )
                container_name = pool_container[0].split("://")[-1]
            else:
                container_name = self.full_container_name

        # run tasks for input files
        task_ids = helpers.add_task_to_job(
            job_id=job_id,
            task_id_base=job_id,
            docker_command=docker_cmd,
            input_files=in_files,
            mounts=self.mounts,
            depends_on=depends_on,
            batch_client=self.batch_client,
            full_container_name=container_name,
            task_id_max=self.task_id_max,
        )
        self.task_id_max += 1
        return task_ids

    def monitor_job(self, job_id: str) -> None:
        """monitor the tasks running in a job

        Args:
            job_id (str): job id
        """
        # monitor the tasks
        monitor = helpers.monitor_tasks(
            job_id,
            self.timeout,
            self.batch_client,
            self.resource_group_name,
            self.account_name,
            self.pool_name,
            self.batch_mgmt_client,
        )
        print(monitor)

        # delete job automatically if debug is false
        if self.debug is False:
            print("Cleaning up - deleting job")
            # Delete job
            self.batch_client.job.delete(job_id)
        elif self.debug is True:
            print("Job complete. Time to debug. Job not deleted.")
            print("**Remember to close out the job when debugging.**")

    def check_job_status(self, job_id: str) -> None:
        """checks various components of a job
        - whether job exists
        - prints number of completed tasks
        - prints the state of a job: completed, activate, etc.

        Args:
            job_id (str): name of job
        """
        # whether job exists
        if helpers.check_job_exists(job_id, self.batch_client):
            print(f"Job {job_id} exists.")
            c_tasks = helpers.get_completed_tasks(job_id, self.batch_client)
            print("Task info:")
            print(c_tasks)
            if helpers.check_job_complete(job_id, self.batch_client):
                print("Job completed.")
            else:
                j_state = helpers.get_job_state(job_id, self.batch_client)
                print(f"Job in {j_state} state")
        else:
            print(f"Job {job_id} does not exist.")

    def delete_job(self, job_id: str) -> None:
        """delete a specified job

        Args:
            job_id (str): job id of job to terminate and delete
        """
        self.batch_client.job.delete(job_id)
        print(f"Job {job_id} deleted.")

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
            return self.full_container_name
        else:
            return None

    def download_file(
        self,
        src_path: str,
        dest_path: str,
        do_check: bool = True,
        container_client=None,
    ) -> None:
        """download a file from Blob storage

        Args:
            src_path (str):
                Path within the container to the desired file (including filename)
            dest_path (str):
                Path to desired location to save the downloaded file
            container (str):
                Name of the storage container containing the file to be downloaded
            do_check (bool):
                Whether or not to do an existence check
            container_client (ContainerClient, optional):
                Instance of ContainerClient provided with the storage account. Defaults to None.
        """
        # use the output container client by default for downloading files
        if container_client is None:
            helpers.download_file(
                self.output_container_client, src_path, dest_path, do_check
            )
        else:
            helpers.download_file(
                container_client, src_path, dest_path, do_check
            )

    def download_directory(
        self, src_path: str, dest_path: str, container_client=None
    ) -> None:
        """download a whole directory from Azure Blob Storage

        Args:
            src_path (str):
                Prefix of the blobs to download
            dest_path (str):
                Path to the directory in which to store the downloads
            container_client (ContainerClient, optional):
                Instance of ContainerClient provided with the storage account. Defaults to None.
        """
        if container_client is None:
            helpers.download_directory(
                self.output_container_client, src_path, dest_path
            )
        else:
            helpers.download_directory(container_client, src_path, dest_path)

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
            print(f"Pool {pool_name} uses {vm_size} VMs.")
            print("Make sure the VM size matches the use case.\n")
        else:
            print(f"Pool {pool_name} does not exist.")
            print("Choose an existing pool or create a new pool.")

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

    def get_pool_full_info(self) -> dict:
        """Retrieve full information about pool used by client.

        Returns:
        - dict: instance of batch_mgmt_client.pool.get()

        """
        pool_info = helpers.get_pool_full_info(
            self.resource_group_name,
            self.account_name,
            self.pool_name,
            self.batch_mgmt_client,
        )
        return pool_info

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
            print(
                "Please specify a blob container or have mounts associated with the client."
            )
            return None
        if blob_container:
            filenames = helpers.list_blobs_flat(
                container_name=blob_container,
                blob_service_client=self.blob_service_client,
                verbose=False,
            )
        elif self.mounts:
            filenames = []
            for mount in self.mounts:
                _files = helpers.list_blobs_flat(
                    container_name=mount[0],
                    blob_service_client=self.blob_service_client,
                    verbose=False,
                )
                filenames += _files
        return filenames
