# Local Module

## Overview
The `local` module in `cfa_azure` was developed to meet the need of easily switching between running code in Azure and executing it locally in a Docker container. Because of the way code and repositories are structured for integration with Azure, it often becomes difficult to then run something locally for testing or other purposes. This module hopes to lighten the shift between Azure and local.

The general idea is that users will have python scripts utilizing `cfa_azure` and will have imports like `from cfa_azure.clients import AzureClient`. Switching to local execution is as easy as changing the import statements to include `.local` after `cfa_azure` in any import, so a local run with the `AzureClient` would now have an import of `from cfa_azure.local.clients import AzureClient`.

The same importing applies to `automation` and `helpers` as well. We would now import these as 
```
from cfa_azure.local import helpers, automation
```

## Things to Consider
There are several things to keep in mind when transitioning between Azure and local execution. The two are not exactly one-to-one (as of now). Not every single function or method in `cfa_azure` exists in `cfa_azure.local`. However, there are enough functions available that a standard workflow can be run locally. Other considerations include:
- creating a pool will create a file in a tmp folder at the path "tmp/pools/<pool_name>.txt"
- creating a job will create a file in a tmp folder at the path "tmp/jobs/<job_name>.txt"
- tasks will be run sequentially in the same Docker container
- tasks are run in the order they are submitted
- creating a blob container will create a new folder in the current working directory. Uploading files to blob storage will copy local files to the new folder.
- depending on the commands used, a Docker container could remain running after jobs are finished.

## Example
Suppose we have the following workflow to create a pool and kick off a couple tasks.
```python
#import client
from cfa_azure.clients import AzureClient
#establish client
client = AzureClient(config_path = "../new_config.toml")
#set pool information
client.set_debugging(False)
client.package_and_upload_dockerfile("cfaprdbatchcr", "test_repo", "latest")
client.set_input_container("input-test", "input")
client.set_blob_container("output-test", "output")
client.set_pool_info("autoscale")
client.create_pool("auto_pool_test")

#create and run job
jobid = "azure_test"
client.add_job(jobid)

client.add_task(jobid, "python3 /input/main.py")
client.add_task(jobid, "python3 /input/summarize.py")
#delete job
client.delete_job(jobid)
```

The same workflow can be run locally simply by changing the first line of code as follows:
```python
#import client from local
from cfa_azure.local.clients import AzureClient
#establish client
client = AzureClient(config_path = "../new_config.toml")
#set pool information
client.set_debugging(False)
client.package_and_upload_dockerfile("cfaprdbatchcr", "test_repo", "latest")
client.set_input_container("input-test", "input")
client.set_blob_container("output-test", "output")
client.set_pool_info("autoscale")
client.create_pool("auto_pool_test")

#create and run job
jobid = "azure_test"
client.add_job(jobid)

client.add_task(jobid, "python3 /input/main.py")
client.add_task(jobid, "python3 /input/summarize.py")
#delete job
client.delete_job(jobid)
```