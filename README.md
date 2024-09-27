# cfa_azure module
## created by Ryan Raasch (Peraton)
##

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

Example:
```
export LOG_LEVEL="info"
export LOG_OUTPUT="stdout"
```
#### Functions
- create_pool: creates a new Azure batch pool using default autoscale mode   
  Example:
  ```
  client = AzureClient("./configuration.toml")
  client.create_pool("My Test Pool")
  ```
- update_scale_settings: modifies the scaling mode (fixed or autoscale) for an existing pool
  Example:
  ```
  # Specify new autoscale formula that will be evaluated every 30 minutes
  client.scaling = "autoscale"
  client.update_scale_settings(
      pool_name="My Test Pool",
      autoscale_formula_path="./new_autoscale_formula.txt", 
      evaluation_interval="PT30M"
  )

  # Set the pool name property to avoid sending pool_name parameter on every call to update_scale_settings
  client.pool_name = "My Test Pool"

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
  client.create_pool(pool_name="My Test pool")

  # Now change the containers mounted on this pool
  client.update_containers(
      pool_name="My Test pool",
      input_container_name="another-input-container",
      output_container_name="another-output-container",
      autoscale_formula_path="./new_autoscale_formula.txt",
      force_update=False
  )
  ```
  If all the nodes in pool were idle when update_containers() method was invoked, Azure Batch service will recreate the pool with new containers mounted to /input and /output paths respectively. However, if any nodes in pool were in Running state, then the following error shall be displayed:

  There are N compute nodes actively running tasks in pool. Please wait for jobs to complete or retry withy force_update=True.

  As the message suggests, you can either wait for existing jobs to complete in the pool and retry the update_containers() operation. Or you can changethe force_update parameter to True and re-run the update_containers() operation to immediately delete the pool and recreate it with new containers. 

 
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
