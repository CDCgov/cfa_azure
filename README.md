# cfa_azure module
## created by Ryan Raasch (Peraton)

## ***Version 1.0.x WARNING***
The expected configuration.toml has changed several keys to make it easier on users to find the right information in the Azure Management Console. The following keys have changed:
- `client_id` is now `batch_application_id`
- `principal_id` is now `batch_object_id`
- `application_id` is now `sp_application_id`

Refer to the example_config.toml in the examples folder, found [here](examples/example_config.toml) to view the required keys/values needed in the configuration file.

# Description
The `cfa_azure` python module is intended to ease the challenge of working with Azure via multiple Azure python modules which require the correct steps and many lines of code to execute. `cfa_azure` simplifies many repeated workflows when interacting with Azure, Blob Storage, Batch, and more. For example, creating a pool in Azure may take different credentials and several clients to complete, but with `cfa_azure`, creating a pool is reduced to a single function with only a few parameters.

# Components
The `cfa_azure` module is composed of three submodules: `batch`, `clients`, and `helpers`. The module `clients` contains what we call the AzureClient, which combines the multiple Azure Clients needed to interact with Azure and consolidates to a single client. The module `helpers` contains more fine-grained functions which are used within the `batch` and `clients` modules or independently for more control when working with Azure.


### clients
Classes:
- AzureClient: a client object used for interacting with Azure. It initializes based on a supplied configuration file and creates various Azure clients under the hood. It can be used to upload containers, upload files, run jobs, and more.

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
```
export LOG_LEVEL="info"
export LOG_OUTPUT="stdout"
```


**Using Various Credential Methods**

When instantiating a AzureClient object, there is an option set the `credential_method` to use. Previously, only a service principal could be used. Now, there is an option to choose `identity`, `sp`, or `env`. Setting `identity` will use the managed identity associated with the VM where the code is running. Setting `sp` will use a service principal for the credential. Setting `env` will use environment variables to create the credential. When choosing `env`, the following environment variables will need to be set: "AZURE_TENANT_ID", "AZURE_CLIENT_ID", and "AZURE_CLIENT_SECRET".

By default, the managed identity option will be used. In whichever credential method is used, a secret is pulled from the keyvault using the credential to create a secretclientcredential for interaction with various Azure services.

**Persisting stdout and stderr to Blob Storage**

In certain situations, it is beneficial to save the stdout and stderr from each task to Blob Storage (like when using autoscale pools). It is possible to persist these to Blob Storage by specifying the blob container name in the `save_logs_to_blob` parameter when using `client.add_job()`. *Note that the blob container specified must be mounted to the pool being used for the job.
For example, if we would like to persist stdout and stderr to the blob container "input-test" for a job named "persisting_test", we would use the following code:
```
client.add_job("persisting_test", save_logs_to_blob = "input-test")
```

**Availability Zones**

To make use of Azure's availability zone functionality there is a parameter available in the `set_pool_info()` method called `availability_zones`. To use availability zones when building a pool, set this parameter to True. If you want to stick with the default Regional configuration, this parameter can be left out or set to False. Turn availability zone on like the following:
```
client.set_pool_info(
  ...
  availability_zones = True,
  ...
)
```

**Updated High Performance Compute Image**
The default base Ubuntu image used for Azure Batch nodes is Ubuntu 20.04, which is nearing end of life on 4/22/2025. There is an option to use a high performance compute image using Ubuntu 22.04 as the base OS. It's important to use a compatible VM size with these HPC images. To implement a HPC image for Azure pools, set the parameter `use_hpc_image` to `True` in the `AzureClient` method `set_pool_info()`, like the following:
```
client.set_pool_info("autoscale",
    timeout=60,
    ...,
    use_hpc_image = True
    )
```

### Functions
- create_pool: creates a new Azure batch pool using default autoscale mode
  Example:
  ```
  client = AzureClient("./configuration.toml")
  client.create_pool("my-test-pool")
  ```
- update_scale_settings: modifies the scaling mode (fixed or autoscale) for an existing pool
  Example:
  ```
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
- update_containers: modifies the containers mounted on an existing Azure batch pool. It essentially recreates the pool with new mounts.
  Example:
  ```
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

 - add_task: adds task to existing job in pool. You can also specify which task it depends on.  By default, dependent tasks will only run if the parent task succeeds. However, this behavior can be overridden by specifying `run_dependent_tasks_on_fail=True` on the parent task. When this property is set to True, any runtime failures in parent task will be ignored. However, execution of dependent tasks will only begin after completion (regardless of success or failure) of the parent task.

  Example: Run tasks in parallel without any dependencies.
  ```
  task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
      input_files=["test_file_1.sh"]
  )
  task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
      use_uploaded_files=False,
      input_files=["test_file_2.sh"]
  )
  ```
 Example: Run tasks sequentially and terminate the job if parent task fails
  ```
  parent_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
      use_uploaded_files=False,
      input_files=["test_file_1.sh"]
  )
  child_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
      use_uploaded_files=False,
      depends_on=parent_task,
      input_files=["test_file_2.sh"]
  )
  ```
 Example: Run tasks sequentially with 1-to-many dependency. Run the child tasks even if parent task fails.
  ```
  parent_task = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
      use_uploaded_files=False,
      run_dependent_tasks_on_fail=True,
      input_files=["test_file_1.sh"]
  )
  child_task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"], # replace with actual command
      depends_on=parent_task,
      input_files=["test_file_2.sh"]
  )
  child_task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["another", "docker", "command"], # replace with actual command
      depends_on=parent_task,
      input_files=["test_file_3.sh"]
  )
  ```
 Example: Create many-to-1 dependency with 2 parent tasks that run before child task. Second parent task is optional: job should not terminate if it fails.
  ```
  parent_task_1 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "docker", "command"],  # replace with actual command
      input_files=["test_file_1.sh"]
  )
  parent_task_2 = client.add_task(
      "test_job_id",
      docker_cmd=["some", "other", "docker", "command"],  # replace with actual command
      run_dependent_tasks_on_fail=True,
      input_files=["test_file_2.sh"]
  )
  child_task = client.add_task(
      "test_job_id",
      docker_cmd=["another", "docker", "command"], # replace with actual command
      depends_on=(parent_task_1 + parent_task_2)
      input_files=["test_file_2.sh"]
  )
  ```


### helpers
Functions:
- read_config: reads in a configuration toml file to a python object
- create_container: creates an Azure Blob container
- get_autoscale_formula: finds and reads autoscale_formula.txt from working directory or subdirectory
- get_sp_secret: retrieves the user's service principal secret
- get_sp_credential: retrieves the service principal credential
- get_blob_service_client: creates a Blob Service Client for interacting with Azure Blob
- get_batch_mgmt_client: creates a Batch Management Client for interacting with Azure Batch
- create_blob_containers: uses create_container() to create input and output containers in Azure Blob
- get_batch_pool_json: creates a dict based on config for working in Azure
- create_batch_pool: creates a Azure Batch Pool based on info stored in the config file
- list_containers: lists the containers in Azure Blob
- upload_files: uploads all files in specified folder to the specified container
- get_batch_service_client: creates a Batch Service Client objectfor interacting with Batch jobs
- add_job: creates a job in Azure Batch
- add_task_to_job: adds a task to the specified job based on user-input Docker command
- monitor_tasks: monitors the tasks running in a job
- list_files_in_container: lists out all files stored in the specified Azure container
- df_to_yaml: converts a pandas dataframe to yaml file
- yaml_to_df: converts a yaml file to pandas dataframe
- edit_yaml_r0: takes in a yaml file and produces replicate yaml files with the r0 changed based on the start, stop, and step provided

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
