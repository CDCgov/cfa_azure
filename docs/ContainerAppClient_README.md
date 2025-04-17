# Using the ContainerAppClient

## Overview
The `ContainerAppClient` found in the `cfa_azure.clients` module is designed to work with Azure Container App Jobs. At this point, this client is not able to create new container app jobs. It can be used to view job information and start existing container app jobs. 

## Instantiating the Client
The `ContainerAppClient` takes in four parameters, with the two of these being required.
- resource_group: the resource group the container app job resides in
- subscription_id: the Azure subscription ID of the container app environment
- job_name: name of container app job (optional)
- credential_method: choice of 'default' and 'managed_identity' for authenticating to Azure. Default is 'default'. (optional)

The client can then be instantiated in the following way:
```python
from cfa_azure.clients import ContainerAppClient

RG = "azure-resource-group"
SUB_ID = "azure-subscription-id"

client = ContainerAppClient(resource_group = RG, subscription_id = SUB_ID)
```

## Methods Available
The following methods are available with the client. In the examples below, assume we have initialized the client as above.

### `get_job_info`
Returns information of the specified container app job.
```python
client.get_job_info(job_name)
```

### `get_command_info`
Returns command, args, and env information for the specified container app job.
```python
client.get_command_info(job_name)
```

### `list_jobs`
Lists all container app jobs contained in the resource group.
```python
client.list_jobs()
```

### `check_job_exists`
Returns True if the specified container app job exists, False otherwise.
```python
client.check_job_exists(job_name)
```

### `start_job`
Submits a request to start the job. The `command`, `args`, and `env` can be changed at runtime to execute something different than the default of the existing container app job. These parameters must be lists of strings.
```python
#execute container app job as it exists
client.start_job(job_name)

#execute with new command
client.start_job(job_name, command = ['python', '-c', 'print("Hello")'])
```