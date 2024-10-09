import datetime
import subprocess as sp

import toml

from cfa_azure import helpers


def create_pool(
    pool_id: str,
    input_container_name: str,
    output_container_name: str,
    config_path: str,
    autoscale_formula_path: str,
):
    """Creates pool(s) in Azure Batch if not exists along with input
    and output containers based on config.

    Args:
        pool_id (str): Name of the pool to use.
        input_container_name (str): Name to be used for input Blob container.
        output_container_name (str): Name to be used for output Blob container.
        config_path (str): Path to config file.
        autoscale_formula_path (str): Path to autoscale formula file.

    Returns:
        json: JSON containing pool_ID and creation_time.
    """

    print("Starting the pool creation process...")

    # Load config
    print("Loading configuration...")
    config = helpers.read_config(config_path)

    # Get credentials
    print("Retrieving service principal credentials...")
    sp_secret = helpers.get_sp_secret(config)
    sp_credential = helpers.get_sp_credential(sp_secret, config)

    # Create blob service account
    print("Setting up Blob service client...")
    blob_service_client = helpers.get_blob_service_client(
        sp_credential, config
    )

    print("Setting up Azure Batch management client...")
    batch_mgmt_client = helpers.get_batch_mgmt_client(sp_credential, config)

    account_name = config["Batch"]["batch_account_name"]
    resource_group_name = config["Authentication"]["resource_group"]

    pool_info = helpers.get_pool_full_info(
        resource_group_name=resource_group_name,
        account_name=account_name,
        pool_name=pool_id,
        batch_mgmt_client=batch_mgmt_client
    )
    vm_config = (pool_info.deployment_configuration.virtual_machine_configuration)
    pool_container = (vm_config.container_configuration.container_image_names)
    container_image_name = pool_container[0].split("://")[-1]

    containers = [
        {'name': input_container_name, 'relative_mount_dir': 'input'},
        {'name': output_container_name, 'relative_mount_dir': 'output'},
    ]
    print("Preparing batch pool configuration...")
    batch_json = helpers.get_batch_pool_json(
        containers=containers,
        config=config,
        autoscale_formula_path=autoscale_formula_path,
        container_image_name=container_image_name
    )

    ####updates
    # take in pool-id
    # check if pool-id already exists in environment
    exists = 0
    try:
        pool_info = batch_mgmt_client.pool.get(
            resource_group_name, account_name, pool_name=pool_id
        )
        exists = 1
    except Exception as e:
        print(e)
    # check if user wants to proceed

    if exists == 1:
        print(f"{pool_id} already exists.")
        print(f"Created: {pool_info.creation_time}")
        print(f"Last modified: {pool_info.last_modified}")
        print(f"VM size: {pool_info.vm_size}")

    # Check if user wants to proceed if the pool already exists
    if exists == 1:
        cont = input("Do you still want to use this pool? [Y/n]:  ")
        if cont.lower() != "y":
            print(
                "No pool created since it already exists. Exiting the process."
            )
            return None

    print("Creating input and output containers...")

    start_time = datetime.datetime.now()

    helpers.create_blob_containers(
        blob_service_client, input_container_name, output_container_name
    )

    print(f"Creating the pool '{pool_id}'...")
    pool_id = helpers.create_batch_pool(batch_mgmt_client, batch_json)
    print(f"Pool '{pool_id}' created successfully.")

    end_time = datetime.datetime.now()
    creation_time = round((end_time - start_time).total_seconds(), 2)
    print(f"Pool creation process completed in {creation_time} seconds.")

    return {
        "pool_id": pool_id,
        "creation_time": creation_time,
    }


def upload_files_to_container(
    folder_names: list[str],
    input_container_name: str,
    blob_service_client: object,
    verbose: bool = False,
    force_upload: bool = False,
):
    """Uploads the files in specified folders to a Blob container.
    Args:
        blob_service_client (object): Blob service client class.
        folder_names (list[str]): A list of folder names from which all files will be uploaded.
        input_container_name (str): Name of input container to upload files to.

    Returns:
        list: List of input file names that were uploaded.
    """
    print(f"Starting to upload files to container: {input_container_name}")
    input_files = []  # Empty list of input files
    for _folder in folder_names:
        # Add uploaded file names to input files list
        uploaded_files = helpers.upload_files_in_folder(
            _folder,
            input_container_name,
            blob_service_client,
            verbose,
            force_upload,
        )
        input_files += uploaded_files
        print(f"Uploaded {len(uploaded_files)} files from {_folder}.")
    print(f"Finished uploading files to container: {input_container_name}")
    return input_files


def run_job(
    job_id: str,
    task_id_base: str,
    docker_cmd: str,
    input_container_name: str,
    output_container_name: str,
    input_files: list[str] | None = None,
    timeout: int = 90,
    config_path: str = "./configuration.toml",
    debug: bool = True,
):
    print(f"Starting job: {job_id}")
    # Load config
    config = toml.load(config_path)

    # Get credentials
    sp_secret = helpers.get_sp_secret()
    sp_credential = helpers.get_sp_credential(sp_secret)

    # Check input_files
    print("Checking input files against container contents...")
    if input_files:
        missing_files = []
        container_files = helpers.list_files_in_container(
            input_container_name, sp_credential, config
        )
        # Check files exist in the container
        for f in input_files:
            if f not in container_files:
                missing_files.append(f)  # Gather list of missing files
        if missing_files:
            print("The following input files are missing from the container:")
            for m in missing_files:
                print(f"    {m}")
            print("Not all input files exist in container. Closing job.")
            return None
    else:
        input_files = helpers.list_files_in_container(
            input_container_name, sp_credential, config
        )
        print(f"All files in container '{input_container_name}' will be used.")

    # Get the batch service client
    batch_client = helpers.get_batch_service_client(sp_secret, config)

    # Add the job to the pool
    print(f"Adding job '{job_id}' to the pool...")
    helpers.add_job(job_id, batch_client, config)

    # Add the tasks to the job
    print(f"Adding tasks to job '{job_id}'...")
    helpers.add_task_to_job(
        job_id, task_id_base, docker_cmd, input_files, batch_client, config
    )

    # Monitor tasks
    print(f"Monitoring tasks for job '{job_id}'...")
    monitor = helpers.monitor_tasks(batch_client, job_id, timeout)
    print(monitor)

    if debug:
        print("Job complete. Time to debug. Job not deleted.")
    else:
        print("Cleaning up - deleting job.")
        batch_client.job.delete(job_id)


def package_and_upload_dockerfile(config: dict):
    """Packages and uploads Dockerfile to Azure Container Registry.

    Args:
        config (dict): Config dictionary with container_account_name and container_name.
    """
    print("Packaging and uploading Dockerfile to Azure Container Registry...")
    container_account_name = config["Container"]["container_account_name"]
    name_and_tag = config["Container"]["container_name"]
    # Execute the shell script to package and upload the container
    result = sp.call(
        [
            "bash",
            "cfa_azure/package_and_upload_container.sh",
            container_account_name,
            name_and_tag,
        ]
    )
    if result == 0:
        print("Dockerfile packaged and uploaded successfully.")
    else:
        print("Failed to package and upload Dockerfile.")
