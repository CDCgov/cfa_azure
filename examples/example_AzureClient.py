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

# set the scaling of the pool:autoscale
client.set_scaling(
    mode="autoscale", autoscale_formula_path="./autoscale_formula.txt"
)
# if fixed mode is desired, do the following:
# client.set_scaling(mode="fixed")

# create the pool
client.create_pool(pool_name="test")
# or use a certain pool
client.use_pool(pool_name="test")

# upload files
client.upload_files_in_folder(["yaml", "input"])

# commad to run the job
client.add_job(job_id="run_test")
docker_cmd = "java -jar /app.jar"
client.add_task(job_id="run_test", docker_cmd=docker_cmd)
client.monitor_job(job_id="run_test")

# close down the jobs, required when using debug is True
# look into https://learn.microsoft.com/en-us/python/api/azure-batch/azure.batch.operations.joboperations?view=azure-python#azure-batch-operations-joboperations terminate
client.delete_job(job_id="run_test")
