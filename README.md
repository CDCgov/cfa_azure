![Version](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2FCDCgov%2Fcfa_azure%2Frefs%2Fheads%2Fmaster%2Fpyproject.toml&query=%24.tool.poetry.version&style=plastic&label=version&color=lightgray)
![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&style=plastic&link=https://raw.githubusercontent.com/CDCgov/cfa_azure/refs/heads/master/.pre-commit-config.yaml)
![pre-commit](https://github.com/CDCgov/cfa_azure/workflows/pre-commit/badge.svg?style=plastic&link=https://github.com/CDCgov/cfa_azure/actions/workflows/pre-commit.yaml)
![CI](https://github.com/CDCgov/cfa_azure/workflows/Python%20Unit%20Tests%20with%20Coverage/badge.svg?style=plastic&link=https://github.com/CDCgov/cfa_azure/actions/workflows/pre-commit.yaml&link=https://github.com/CDCgov/cfa_azure/actions/workflows/ci.yaml)
![GitHub License](https://img.shields.io/github/license/cdcgov/cfa_azure?style=plastic&link=https://github.com/CDCgov/cfa_azure/blob/master/LICENSE)
![Python](https://img.shields.io/badge/python-3670A0?logo=python&logoColor=ffdd54&style=plastic)
![Azure](https://img.shields.io/badge/Microsoft-Azure-blue?logo=microsoftazure&logoColor=white&style=plastic)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/cdcgov/cfa_azure?style=plastic)


# cfa_azure Python Package
## Created by Ryan Raasch (Peraton) for CFA

# Outline
- [Recent Updates](#recent-updates)
- [Description](#description)
- [Getting Started](#getting-started)
- [Components](#components)
  - [clients](#clients)
    - [Logging](#logging)
    - [Using Various Credential Methods](#using-various-credential-methods)
    - [Persisting stdout and stderr to Blob Storage](#persisting-stdout-and-stderr-to-blob-storage)
    - [Availability Zones](#availability-zones)
    - [Updated Base Container Image](#updated-base-container-image)
    - [Configuration](#configuration)
    - [AzureClient Methods](#azureclient-methods)
    - [Running Jobs and Tasks](#running-jobs-and-tasks)
    - [Running Jobs and Tasks with Timeout](#running-jobs-and-tasks-with-timeout)
    - [Running Tasks from Yaml](#running-tasks-from-yaml)
    - [Download Blob Files After Job Completes](#download-blob-files-after-job-completes)
    - [Run DAGs](#run-dags)
  - [automation](#automation)
  - [local](#local)
  - [batch_helpers](#batch_helpers)
    - [Batch Helpers Functions](#batch-helpers-functions)
  - [blob_helpers](#blob_helpers)
    - [Blob Helpers Functions](#blob-helpers-functions)
  - [helpers](#helpers)
    - [Helpers Functions](#helpers-functions)
  - [Common Use Case Scenarios](#common-use-case-scenarios)
- [Warnings](#warnings)
- [Public Domain Standard Notice](#public-domain-standard-notice)
- [License Standard Notice](#license-standard-notice)
- [Privacy Standard Notice](#privacy-standard-notice)
- [Contributing Standard Notice](#contributing-standard-notice)
- [Records Management Standard Notice](#records-management-standard-notice)
- [Additional Standard Notices](#additional-standard-notices)

# Recent Updates

## v1.5.9
Accommodated updates to the pygriddler package. If using a parameters yaml file for the automation, it must include 'schema: v0.3' at the top of the file instead of using the 'version' key.

## v1.5.3
Added timeout parameter for `AzureClient.add_task` and `AzureClient.add_job` methods.
Updated `AzureClient.monitor_job` to provide more detail in output.

## v1.5.0
Added integration for running container app jobs via `cfa_azure.clients.ContainerAppClient`.

# Description
The `cfa_azure` python module is intended to ease the challenge of working with Azure via multiple Azure python modules which require the correct steps and many lines of code to execute. `cfa_azure` simplifies many repeated workflows when interacting with Azure, Blob Storage, Batch, Container App Jobs and more. For example, creating a pool in Azure may take different credentials and several clients to complete, but with `cfa_azure`, creating a pool is reduced to a single function with only a few parameters.

# Getting Started
In order to use the `cfa_azure` library, you need [Python 3.10 or higher](https://www.python.org/downloads/), [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/), and any python package manager.

To install using pip:
```bash
pip install git+https://github.com/CDCgov/cfa_azure.git
```

# Components
The `cfa_azure` module is composed of the following submodules: `clients`, `automation`,  `helpers`, `batch_helpers`, `blob_helpers`, `batch`, and `local`. The module `clients` contains what we call the AzureClient and the ContainerAppClient. The AzureClient combines the multiple Azure Clients needed to interact with Azure and consolidates to a single client. The ContainerAppClient provides a simple way to view and start container app jobs. The three helpers modules contain more fine-grained functions which are used within the `clients` module or independently for more control when working with Azure. The `automation` module introduces a simplified way to upload files and submit jobs/tasks to Batch via another configuration toml file. For help getting started with the `automation` module, please see [this overview](docs/automation_README.md).

The `local` submodule is meant to mimic the `cfa_azure` package but in a local environment, and contains submodules also called `client`, `automation` and `helpers`. This framework allows for users to easily switch between running code in Azure and locally. For example, someone with a working script importing the `AzureClient` by running `from cfa_azure.clients import AzureClient` could switch to running it locally by importing it through the `local` submodule like `from cfa_azure.local.clients import AzureClient`. The same holds for `local.automation` and `local.helpers`.

**Note:** At this moment, not all functionality in `cfa_azure` is available in the `local` submodule, but there is enough for a standard workflow to be ran locally.

# Module Tree
```
|cfa_azure
    | clients
        | AzureClient
        | ContainerAppClient
    | automation
    | batch_helpers
    | blob_helpers
    | helpers
    | local
        | clients
            | AzureClient
        | automation
        | helpers
```

## clients
Classes:
- AzureClient: a client object used for interacting with Azure. It initializes based on a supplied configuration file and creates various Azure clients under the hood. It can be used to upload containers, upload files, run jobs, and more.
- ContainerAppClient: a client object used for viewing and starting container app jobs. Documentation available [here](/docs/ContainerAppClient_README.md)

### Logging
To customize the logging capabilities of cfa_azure, two environment variables can be set. These are LOG_LEVEL and LOG_OUTPUT.

LOG_LEVEL: sets the logging level. Choices are:
- debug
- info
- warning
- error

LOG_OUTPUT: sets the output of the logs. Choices are:
- file: saves log output to a file, nested within a ./logs/ folder
- stdout: saves log output to stdout
- both: saves log output to both file and stdout

**Example**:
Run the following in the terminal in which `cfa_azure` will be run.
```bash
export LOG_LEVEL="info"
export LOG_OUTPUT="stdout"
```


### Using Various Credential Methods

When instantiating a AzureClient object, there is an option to set which `credential_method` to use. Previously, only a service principal could be used. Now, there are three an options to choose `identity`, `sp`, or `env`.
- `identity`: Uses the managed identity associated with the VM where the code is running.
- `sp`: Uses a service principal for the credential. The following values must be set in the configuration file: tenant_id, sp_application_id, and the corresponding secret fetched from Azure Key Vault.
- `env`: Uses environment variables to create the credential. When choosing `env`, the following environment variables will need to be set: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`.

You can also use `use_env_vars=True` to allow the configuration to be loaded directly from environment variables, which may be helpful in containerized environments.

By default, the managed identity option will be used. In whichever credential method is used, a secret is pulled from the key vault using the credential to create a secret client credential for interaction with various Azure services.

**Example:**
```python
from cfa_azure.clients import AzureClient

# Using Managed Identity
client = AzureClient(config_path="./configuration.toml", credential_method="identity")

# Using Service Principal (credentials from the config file)
client = AzureClient(config_path="./configuration.toml", credential_method="sp")

# Using Environment Variables
import os
os.environ["AZURE_TENANT_ID"] = "your-tenant-id"
os.environ["AZURE_CLIENT_ID"] = "your-client-id"
os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret" #pragma: allowlist secret
client = AzureClient(credential_method="env", use_env_vars=True)
```

### Persisting stdout and stderr to Blob Storage

In certain situations, it is beneficial to save the stdout and stderr from each task to Blob Storage (like when using autoscale pools). It is possible to persist these to Blob Storage by specifying the blob container name in the `save_logs_to_blob` parameter when using `client.add_job()`. *Note that the blob container specified must be mounted to the pool being used for the job.

For example, if we would like to persist stdout and stderr to the blob container "input-test" for a job named "persisting_test", we would use the following code:
```python
client.add_job("persisting_test", save_logs_to_blob = "input-test")
```

### Availability Zones

To make use of Azure's availability zone functionality there is a parameter available in the `set_pool_info()` method called `availability_zones`. To use availability zones when building a pool, set this parameter to True. If you want to stick with the default Regional configuration, this parameter can be left out or set to False. Turn availability zone on like the following:
```python
client.set_pool_info(
  ...
  availability_zones = True,
  ...
)
```

### Updated Base Container Image

The original base Ubuntu image used for Azure Batch nodes was Ubuntu 20.04, which is deprecated effective April 2025. There is a new image provided by default from `microsoft-dsvm`, which runs Ubuntu 22.04 for container workloads. This new image supports high performance compute (HPC) VMs as well as a limited number of non-HPC VMs. Going forward, `cfa_azure` will only support the creation of pools with the new `microsoft-dsvm` image.
The following non-HPC VMs can be used with the updated image:
- d2s_v3
- d4s_v3
- d4d_v5
- d4ds_v5
- d8s_v3
- d16s_v3
- d32s_v3
- e8s_v3
- e16s_v3

There may be other compatible VMs as well, but note that the A-series VMs are no longer compatible.

**Note:** all pools will need to be updated to the newer image by mid-April 2025.


### Configuration
An AzureClient object can be instantiated and initialized with pool, mounted containers and container registries using a configuration file. This is especially useful if the same pool will be used for running multiple batch jobs and experiments. Use the following example to create a configuration file:

[Configuration File](examples/client_configuration.toml)


After creating the configuration file (e.g. client_configuration.toml), then use the following snippet to initialize the AzureClient object:
```python
  client = AzureClient("./client_configuration.toml")
```

### AzureClient Methods
- `create_pool`: creates a new Azure batch pool using default autoscale mode
  **Example:**
  ```python
  client = AzureClient("./configuration.toml")
  client.create_pool("my-test-pool")
  ```
- `download_job_stats`: downloads a csv of job statistics for the specified job in its current state, to the specified file_name if provided (without the .csv extension). If no file_name is provided, the csv is downloaded to {job_id}-stats.csv. There is also a parameter in the `monitor_job()` method with the same name that, when set to True, will save the job statistics when the job completes. Examples:
```python
client.download_job_stats(job_id = "example-job-name", file_name = "test-job-stats")

client.monitor_job(job_id = "example-job-name", download_job_stats = True)
```
- `update_containers`: modifies the containers mounted on an existing Azure batch pool. It essentially recreates the pool with new mounts. Use force_update=True to recreate the pool without waiting for running tasks to complete.
- `upload_files_to_container`: uploads files from a specified folder to an Azure Blob container. It also includes options like `force_upload` to allow or deny large file uploads without confirmation.
  **Example:**
```python
client.upload_files_to_container(
    folder_names=["/path/to/folder"],
    input_container_name="my-input-container",
    blob_service_client=client.blob_service_client,
    force_upload=True
)
```
- `update_scale_settings`: modifies the scaling mode (fixed or autoscale) for an existing pool
 **Example:**
  ```python
  # Specify new autoscale formula that will be evaluated every 30 minutes
  client.scaling = "autoscale"
  client.update_scale_settings(
      pool_name="my-test-pool",
      autoscale_formula_path="./new_autoscale_formula.txt",
      evaluation_interval="PT30M"
  )

  # Set the pool name property to avoid sending pool_name parameter on every call to update_scale_settings
  client.pool_name = "my-test-pool"

  # Use default 15 minute evaluation interval
  client.update_scale_settings(autoscale_formula_path="./new_autoscale_formula.txt")

  # Switch to fixed scaling mode with 10 on-demand EC2 nodes and requeuing of current jobs
  client.scaling = "fixed"
  client.update_scale_settings(dedicated_nodes=10, node_deallocation_option='Requeue')

  # Switch to fixed scaling mode with 15 spot EC2 nodes and forced termination of current jobs
  client.update_scale_settings(low_priority_nodes=15, node_deallocation_option='Terminate')
  ```
- `update_containers`: modifies the containers mounted on an existing Azure batch pool. It essentially recreates the pool with new mounts.
 **Example:**
  ```python
  # First create a pool
  client = AzureClient("./configuration.toml")
  client.set_input_container("some-input-container")
  client.set_output_container("some-output-container")
  client.create_pool(pool_name="my-test-pool")

  # Now change the containers mounted on this pool
  client.update_containers(
      pool_name="my-test-pool",
      input_container_name="another-input-container",
      output_container_name="another-output-container",
      force_update=False
  )
  ```
  If all the nodes in pool were idle when update_containers() method was invoked, Azure Batch service will recreate the pool with new containers mounted to /input and /output paths respectively. However, if any nodes in pool were in Running state, then the following error shall be displayed:

  *There are N compute nodes actively running tasks in pool. Please wait for jobs to complete or retry with `force_update=True`.*

  As the message suggests, you can either wait for existing jobs to complete in the pool and retry the `update_containers()` operation. Or you can change the `force_update` parameter to `True and re-run the `update_containers()` operation to immediately recreate the pool with new containers.

### Running Jobs and Tasks
 - `add_task`: adds task to existing job in pool. You can also specify which task it depends on.  By default, dependent tasks will only run if the parent task succeeds. However, this behavior can be overridden by specifying `run_dependent_tasks_on_fail=True` on the parent task. When this property is set to True, any runtime failures in parent task will be ignored. However, execution of dependent tasks will only begin after completion (regardless of success or failure) of the parent task.

 **Example:** Run tasks in parallel without any dependencies.
  ```python
  task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
  )
  task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
  )
  ```
**Example:** Run tasks sequentially and terminate the job if parent task fails
  ```python
  parent_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
  )
  child_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
      depends_on=parent_task,
  )
  ```
**Example:** Run tasks sequentially with 1-to-many dependency. Run the child tasks even if parent task fails.
  ```python
  parent_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
      run_dependent_tasks_on_fail=True,
  )
  child_task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
      depends_on=parent_task,
  )
  child_task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["another", "docker", "command"], # replace with actual command
      depends_on=parent_task,
  )
  ```
**Example:** Create many-to-1 dependency with 2 parent tasks that run before child task. Second parent task is optional: job should not terminate if it fails.
  ```python
  parent_task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
  )
  parent_task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"],  # replace with actual command
      run_dependent_tasks_on_fail=True,
  )
  child_task = client.add_task(
      "test_job_id",
      docker_cmd=["another", "docker", "command"], # replace with actual command
      depends_on=[parent_task_1, parent_task_2]
  )
  ```

  **Example**: Use integer values for task IDs and specify dependent tasks as a range.
  ```python
  #create job
client.add_job(job_id = "task_dep_range",task_id_ints = True)
#submit tasks
for item in range(20):
    <submit tasks>

#add dependent task which depends on tasks 1 to 20.
client.add_task("python3 some_cmd.py", depends_on_range = (1, 20))
  ```

### Running Jobs and Tasks with Timeout
Jobs and tasks can also be given a timeout value (in minutes) to prevent jobs/tasks from running too long. For example, a certain job should take less than 30 minutes or each task should take no more than 5 minutes. The following code can be used in this instance:
```python
client.add_job("job_timeout_example", timeout = 30)
client.add_task("python3 run_task.py", job_id = "job_timeout_example", timeout = 5)
```

### Running Tasks from Yaml
Tasks can also be added to a job based on a yaml file containing various parameters and flags. The yaml is parsed into command line arguments and appended to a base command to be used as the docker command in Azure Batch. The yaml/argument parsing utilizes [pygriddler](https://github.com/CDCgov/pygriddler). The basic structure for this method is `client.add_tasks_from_yaml(job_id, base_cmd, file_path)`.
For example, a yaml called params.yaml that has the following structure
```yaml
baseline_parameters:
  p_infected_initial: 0.001

grid_parameters:
  scenario: [pessimistic, optimistic]
  run: [1, 2, 3]

nested_parameters:
  - scenario: pessimistic
    R0: 4.0
    p_infected_initial: 66
    infectious_period: 2.0
    infer(flag): x
    run_checks(flag): x
  - scenario: optimistic
    R0: 2.0
    infectious_period: 0.5
```
run with the following method
```python
client.add_tasks_from_yaml(job_id = "args_example",
  base_cmd = "python3 main.py",
  file_path = "params.yaml"
  )
```
will produce 6 tasks with the following docker_cmds passed to Batch:
```bash
'python3 main.py  --scenario pessimistic --run 1 --p_infected_initial 66 --R0 4.0 --infectious_period 2.0 --infer --run_checks'
'python3 main.py  --scenario pessimistic --run 2 --p_infected_initial 66 --R0 4.0 --infectious_period 2.0 --infer --run_checks'
'python3 main.py  --scenario pessimistic --run 3 --p_infected_initial 66 --R0 4.0 --infectious_period 2.0 --infer --run_checks'
'python3 main.py  --scenario optimistic --run 1 --p_infected_initial 0.001 --R0 2.0 --infectious_period 0.5'
'python3 main.py  --scenario optimistic --run 2 --p_infected_initial 0.001 --R0 2.0 --infectious_period 0.5'
'python3 main.py  --scenario optimistic --run 3 --p_infected_initial 0.001 --R0 2.0 --infectious_period 0.5'
```

### Download Blob Files After Job Completes
Sometimes there will be outputs from a job that you know will need to be downloaded locally. This can be accomplished by using the `download_after_job()` method. It accepts `job_id`, `blob_paths`, `target`, and `container_name` as parameters. This method should be placed at the end of your script after submitting tasks so that it monitors the job and downloads the specified output when the tasks finish running. Blob paths can be directories or specific file paths. The contents of a director will be downloaded keeping the structure of the directory that exists in Blob Storage.
Example:
```python
client.download_after_job(
  job_id = "sample_job",
  blob_paths = ["folder1", "folder2/subfolder", file.txt"],
  target = "dload",
  container_name = "output-test"
)
```

### Run DAGs
An instance of the AzureClient can run DAGs in a user-specified job. It takes in Task objects from the `batch` module, along with a job_id and other `add_task()` parameters. It determines which order to submit the tasks and sets the appropriate dependencies in Azure Batch. See [the DAGs documentation](/examples/DAGs/README.md) for more information.

## automation
Please view [this documentation](docs/automation_README.md) on getting started with the `automation` module.

## local
Please view [this documentation](docs/local_README.md) for more information regarding the `local` module.

## Helper functions
The CFA Azure library provides a collection of functions that help manage Azure Batch, Blob Storage, Identity Management and Configuration. These functions have been grouped into 3 different modules: `batch_helpers`, `blob_helpers` and `helpers`. In the following sections, each module and its functions are described.

### batch_helpers
The `batch_helpers` module provides a collection of functions that helps manage Azure Batch resources and perform key tasks. Below is an expanded overview of each function.

#### Batch Helpers Functions
- `check_pool_exists`: checks if a specified pool exists in Azure Batch
```python
check_pool_exists("resource_group_name", "account_name", "pool_name", batch_mgmt_client)
```
- `create_batch_pool`: creates a Azure Batch Pool based on info using the provided configuration details
```python
create_batch_pool(batch_mgmt_client, pool_config)
```
- `delete_pool`: deletes the specified pool from Azure Batch
```python
delete_pool("resource_group_name", "account_name", "pool_name", batch_mgmt_client)
```
- `generate_autoscale_formula`: generates a generic autoscale formula for use based on a specified maximum number of nodes
```python
generate_autoscale_formula(max_nodes=8)
```
- `get_autoscale_formula`: finds and reads `autoscale_formula.txt` from working directory or subdirectory
```python
get_autoscale_formula(filepath="/path/to/formula.txt")
```
- `get_batch_mgmt_client`: creates a Batch Management Client for interacting with Azure Batch, such as pools and jobs
```python
batch_mgmt_client = get_batch_mgmt_client(config, DefaultAzureCredential())
```
- `get_batch_pool_json`: creates a dict based on config for configuring an Azure Batch pool
```python
pool_config = get_batch_pool_json("input-container", "output-container", config)
```
- `get_deployment_config`: retrieves deployment configuration for Azure Batch pool, including container registry settings and optional HPC image
```python
get_deployment_config("container_image_name", "container_registry_url", "container_registry_server", config, DefaultAzureCredential())
```
- `get_network_config`: gets the network configuration based on the config information
```python
get_network_config(config: str)
```
- `get_pool_full_info`: retrieves the full information of a specified Azure Batch pool
```python
get_pool_full_info("resource_group_name", "account_name", "pool_name", batch_mgmt_client)
```
- `get_pool_info`: gets the basic information for a specified Azure Batch pool
```python
get_pool_info("resource_group_name", "account_name", "pool_name", batch_mgmt_client)
```
- `get_pool_mounts`: lists all mounted Blob containers for a given Azure Batch pool
```python
get_pool_mounts("pool_name", "resource_group_name", "account_name", batch_mgmt_client)
```
- `get_rel_mnt_path`: retrieves the relative mount path for a specified Blob container in an Azure Batch pool
```python
get_rel_mnt_path("blob_name", "pool_name", "resource_group_name", "account_name", batch_mgmt_client)
```
- `get_user_identity`: retrieves the user identity based on the provided config information
```python
get_user_identity(config)
```

### blob_helpers
The `blob_helpers` module provides a collection of functions that helps manage Azure Blob Storage resources and perform key tasks. Below is an expanded overview of each function.

#### Blob Helpers Functions
- `blob_glob`: provides an iterator over all files within specified Azure Blob Storage location that match the specified prefix.
```python
blob_glob("blob_url", "account_name", "container_name", "container_client")
```
- `blob_search`: provides an iterator over all files within specified Azure Blob Storage location that match the specified prefix and file pattern. It can optionally take a sort key.
```python
blob_search("blob_url", "account_name", "container_name", "container_client")
blob_search("blob_url", "account_name", "container_name", "container_client", "sort_key")
```
**Example: List Azure blob files from a folder**
```python
from cfa_azure.blob_helpers import blob_glob
for blob in blob_glob("src/dynode/mechanistic*.py", account_name='cfaazurebatchprd', container_name='input'):
    print(blob)

# sort all files within input/ folder by last_modified date and display name
for blob in blob_glob('input/', account_name='cfaazurebatchprd', container_name='input-test', sort_key='last_modified'):
    print(blob['name'])

# sort all markdown files by last_modified date and display name
for blob in blob_glob('*.md', account_name='cfaazurebatchprd', container_name='input-test', sort_key='last_modified'):
    print(blob['name'])
```
```
- `read_blob_stream`: reads file from specified path in Azure Storage and return its contents as bytes without mounting the container to a local filesystem
```python
read_blob_stream("blob_url", "account_name", "container_name", "container_client")
```
**Example: Read Azure blob file into Polars or Pandas data frames**
```python
from cfa_azure.blob_helpers import read_blob_stream
data_stream = read_blob_stream("input/AZ.csv", account_name='cfaazurebatchprd', container_name='input-test')

# Read into Polars dataframe
import polars
df = polars.read_csv(data_stream.readall())
print(df)

# Read into Pandas dataframe
import pandas
df = pandas.read_csv(data_stream)
print(df)

# Read large file into Pandas dataframe within chunking
import pandas
chunk_size=1000      # 1000 rows at a time
for chunk in pd.read_csv(data_stream, chunksize=chunk_size):
    print(chunk)
```
- `write_blob_stream`: write bytes to a file in specified path
```python
write_blob_stream("data", "blob_url", "account_name", "container_name", "container_client")
```
**Example: Write Polars or Pandas dataframe into Azure blob storage**
```python
from cfa_azure.blob_helpers import write_blob_stream

# Write Polars dataframe
import polars
df = .... # Read some data into Polars dataframe
blob_url = "input/AZ_03072025_a.csv"
write_blob_stream(df.write_csv().encode('utf-8'), blob_url=blob_url, account_name='cfaazurebatchprd', container_name='input-test')

# Write Pandas dataframe
import pandas
df = .... # Read some data into Pandas dataframe
data = df.to_csv(index=False).encode('utf-8')
blob_url = "input/AZ_03072025_a.csv"
write_blob_stream(data, blob_url=blob_url, account_name='cfaazurebatchprd', container_name='input-test')
```
- `check_blob_existence`: checks whether a blob exists in the specified container
```python
check_blob_existence(c_client, "blob_name")
```
- `check_virtual_directory_existence`: checks whether any blobs exist with the specified virtual directory path
```python
check_virtual_directory_existence(c_client, "vdir_path")
```
- `create_blob_containers`: uses create_container() to create input and output containers in Azure Blob
```python
create_blob_containers(blob_service_client, "input-container", "output-container")
```
- `delete_blob_snapshots`: deletes a blob and all its snapshots in a container
```python
delete_blob_snapshots("blob_name", "container_name", blob_service_client)
```
- `delete_blob_folder`: deletes all blobs in a specified folder in a container
```python
delete_blob_folder("folder_path", "container_name", blob_service_client)
```
- `download_file`: downloads a file from Azure Blob storage to a specified location
```python
download_file(c_client, "src_path", "dest_path")
```
- download_directory: downloads a directory using prefix matching from Azure Blob storage
```python
download_directory("container_name", "src_path", "dest_path", blob_service_client, include_extensions=".txt", verbose=True)
```
- `format_extensions`: formats file extensions into a standard format for use
```python
format_extensions([".txt", "jpg"])
```
- `get_blob_service_client`: creates a Blob Service Client for interacting with Azure Blob
```python
blob_service_client = get_blob_service_client(config, DefaultAzureCredential())
```
- `list_blobs_flat`: lists all blobs in a specified container
```python
list_blobs_flat("container_name", blob_service_client)
```
- `list_containers`: lists the containers in Azure Blob Storage Account
```python
list_containers(blob_service_client)
```
- `upload_blob_file`: uploads a specified file to Azure Blob storage
```python
upload_blob_file("file_path", location="folder/subfolder", container_client=container_client, verbose=True)
```
- `upload_files_in_folder`: uploads all files in specified folder to the specified container
```python
upload_files_in_folder("/path/to/folder", "container-name", blob_service_client)
```

### helpers
The `helpers` module provides a collection of functions that helps manage Azure resources and perform key tasks, such as interacting with configuration management, and data transformations. Below is an expanded overview of each function.

#### Helpers Functions
- `read_config`: reads in a configuration toml file and returns it as a Python dictionary
```python
read_config("/path/to/config.toml")
```
- `create_container`: creates an Azure Blob container if it doesn't already exist
```python
create_container("my-container", blob_service_client)
```
- `get_sp_secret`: retrieves the user's service principal secret from the key vault based on the provided config file
```python
get_sp_secret(config, DefaultAzureCredential())
```
- `get_sp_credential`: retrieves the service principal credential
```python
get_sp_credential(config)
```
- `get_batch_service_client`: creates a Batch Service Client object for interacting with Batch jobs
```python
batch_client = get_batch_service_client(config, DefaultAzureCredential())
```
- `add_job`: creates a new job to the specified Azure Batch pool. By default, a job remains active after completion of enclosed tasks. You can optionally specify the *mark_complete_after_tasks_run* argument to *True* if you want job to auto-complete after completion of enclosed tasks.
```python
add_job("job-id", "pool-id", True, batch_client)
```
- `add_task_to_job`: adds a task to the specified job based on user-input Docker command
```python
add_task_to_job("job-id", "task-id", "docker-command", batch_client)
```
- `monitor_tasks`: monitors the tasks running in a job
```python
monitor_tasks("example-job-id", batch_client)
```
- `list_files_in_container`: lists out all files stored in the specified Azure container
```python
list_files_in_container(container_client)
```
- `df_to_yaml`: converts a pandas dataframe to yaml file, which is helpful for configuration and metadata storage
```python
df_to_yaml(dataframe, "output.yaml")
```
- `yaml_to_df`: converts a yaml file to pandas dataframe
```python
yaml_to_df("input.yaml")
```
- `edit_yaml_r0`: takes in a YAML file and produces replicate YAML files with the `r0` changed based on the specified range (i.e. start, stop, and step)
```python
edit_yaml_r0("input.yaml", start=1, stop=5, step=1)
```
- `get_log_level`: retrieves the logging level from environment variables or defaults to debug
```python
get_log_level()
```
- `check_autoscale_parameters`: checks which arguments are incompatible with the provided scaling mode
```python
check_autoscale_parameters("autoscale", dedicated_nodes=5)
```
- `get_rel_mnt_path`: retrieves the relative mount path for a specified Blob container in an Azure Batch pool
```python
get_rel_mnt_path("blob_name", "pool_name", "resource_group_name", "account_name", batch_mgmt_client)
```
- `check_env_req`: checks if all necessary environment variables exist for the Azure client
```python
check_env_req()
```
- `check_config_req`:checks if the provided configuration file contains all necessary components for the Azure client
```python
check_config_req(config)
```
- `get_container_registry_client`: retrieves a Container Registry client for Azure
```python
get_container_registry_client("endpoint", DefaultAzureCredential(), "audience")
```
- `check_azure_container_exists`: checks if a container with the specified name, repository, and tag exists in Azure Container Registry
```python
check_azure_container_exists("registry_name", "repo_name", "tag_name", DefaultAzureCredential())
```
- `format_rel_path`: formats a given relative path by removing the leading slash if present
```python
format_rel_path("/path/to/resource")
```
- `get_timeout`: converts a given duration string (in ISO 8601 format) to minutes
```python
get_timeout("PT1H30M")
```
- `check_job_exists`: checks whether a job with the specified ID exists in Azure Batch
```python
check_job_exists("job_id", batch_client)
```
- `get_completed_tasks`: returns the number of completed tasks for the specified job
```python
get_completed_tasks("job_id", batch_client)
```
- `check_job_complete`: checks if the specified job is complete
```python
check_job_complete("job_id", batch_client)
```
- `get_job_state`: returns the state of the specified job, such as 'completed' or 'active'
```python
get_job_state("job_id", batch_client)
```
- `package_and_upload_dockerfile`: packages a Dockerfile and uploads it to the specified registry and repo with the designated tag
```python
package_and_upload_dockerfile("registry_name", "repo_name", "tag", use_device_code=True)
```
- `upload_docker_image`: uploads a Docker image to a specified Azure Container Registry repo with an optional tag
```python
upload_docker_image("image_name", "registry_name", "repo_name", tag="latest", use_device_code=False)
```

## Common Use Case Scenarios

**Example Workflow**: Uploading files to Blob Storage, creating an Azure Batch Pool, adding jobs, and monitoring tasks.

```python
# Step 1: Read configuration
config = read_config("config.toml")

# Step 2: Create Blob containers
blob_service_client = get_blob_service_client(config, DefaultAzureCredential())
create_blob_containers(blob_service_client, "input-container", "output-container")

# Step 3: Upload files to the container
upload_files_in_folder("/path/to/folder", "input-container", blob_service_client)

# Step 4: Create an Azure Batch Pool
batch_mgmt_client = get_batch_mgmt_client(config, DefaultAzureCredential())
pool_config = get_batch_pool_json("input-container", "output-container", config)
create_batch_pool(batch_mgmt_client, pool_config)

# Step 5: Create a job and add tasks
batch_client = get_batch_service_client(config, DefaultAzureCredential())
add_job("job-id", "pool-id", True, batch_client)
add_task_to_job("job-id", "task-id", "docker command", batch_client)

# Step 6: Monitor the tasks
monitor_tasks("job-id", batch_client)
```

# Warnings
## ***Version 1.x.x WARNING***
The expected configuration.toml has changed several keys to make it easier on users to find the right information in the Azure Management Console. The following keys have changed:
- `client_id` is now `batch_application_id`
- `principal_id` is now `batch_object_id`
- `application_id` is now `sp_application_id`

Refer to the example_config.toml in the examples folder, found [here](examples/example_config.toml) to view the required keys/values needed in the configuration file.

## ***Version 1.3.x WARNING***
The method `add_task()` no longer accepts parameters `use_uploaded_files` or `input_files`. Any files will need to be accounted for when specifying the docker command to run the task.

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](DISCLAIMER.md)
and [Code of Conduct](code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo) or creating a new branch
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

Help make this package/repo more robust and stable by creating issues as you see fit. Please use the following issues template as an outline for your issue: [issue template](.github/ISSUE_TEMPLATE/cfa_azure_issue_template.md)

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

## Additional Standard Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/main/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/main/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/main/code-of-conduct.md).
