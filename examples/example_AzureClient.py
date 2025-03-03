# python

from cfa_azure.clients import AzureClient

# initialize the AzureClient with config
client = AzureClient(config_path="./configuration.toml")

# turn debugging on, this is required
client.set_debugging(True)

client.package_and_upload_dockerfile(
    registry_name="test_registry", repo_name="repo1", tag="test"
)

# create the input and output containers
client.create_input_container("example-input", "input")
client.create_output_container("example-output", "output")

# or create/set additional blob containers
client.create_blob_container("containername", "/path")
client.set_blob_container("containername", "/path")

# set the scaling of the pool:autoscale
client.set_pool_info(
    mode="autoscale", autoscale_formula_path="./autoscale_formula.txt"
)
# if fixed mode is desired, do the following:
# client.set_scaling(mode="fixed")

# create the pool
client.create_pool(pool_name="test")
# or set a certain pool
client.set_pool(pool_name="test")

# upload files
client.upload_files_in_folder(["yaml", "input"])

# get names of files that exist in blob storage
client.list_blob_files()

# commad to run the job
client.add_job(job_id="run_test", end_job_on_task_failure=True)
docker_cmd = "java -jar /app.jar"
client.add_task(job_id="run_test", docker_cmd=docker_cmd)
client.monitor_job(job_id="run_test")

# download job output from blob storage to local
client.download_after_job(
    job_id="run_test",
    blob_paths=["folder1", "folder/subfolder", "file.txt"],
    target="local_folder",
    container_name="output-test",
)

# close down the jobs, required when using debug is True
client.delete_job(job_id="run_test")
