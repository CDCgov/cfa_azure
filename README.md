# cfa_azure module - not ready for production use
## created by Ryan Raasch (Peraton)
### version 0.1.0
##

The `cfa_azure` python module is intended to ease the challenge of working with Azure via multiple Azure python modules which require the correct steps and many lines of code to execute. `cfa_azure` simplifies many repeated workflows when interacting with Azure, Blob Storage, Batch, and more. For example, creating a pool in Azure may take different credentials and several clients to complete, but with `cfa_azure`, creating a pool is reduced to a single function with only a few parameters.

# Components
The `cfa_azure` module is composed of three submodules: `batch`, `clients`, and `helpers`. The module `batch` contains most of the main functions for interacting with Azure and, specifically, Azure Batch. The module `clients` contains what we call the AzureClient, which combines the multiple Azure Clients needed to interact with Azure and consolidates to a single client. The module `helpers` contains more fine-grained functions which are used within the `batch` and `clients` modules or independently for more control when working with Azure.

## batch
Functions:
- create_pool: creates input and output containers and sets up Azure pool based on config.
- upload_files_to_container: uploads files from specified folders to the specified Azure Blob container.
- run_job: runs and monitors a job in Azure Batch based on user input Docker command.
- package_and_upload_dockerfile: packages container specified in Dockerfile and uploads to Azure Container Registry based on info in config file.

## clients
Classes:
- AzureClient: a client object used for interacting with Azure. It initializes based on a supplied configuration file and creates various Azure clients under the hood. It can be used to upload containers, upload files, run jobs, and more.

## helpers
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
