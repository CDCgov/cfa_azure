# Reading and Writing from Azure Blob Storage in Batch Job
## Created by Fawad Rafi (Peraton) for CFA

# Outline
Developers can use `read_blob` and `write_blob` methods of `AzureClient` class to read and writes files from within a Docker container. The container may be launched either in Docker Desktop running on VAP or as a job in Azure Batch service. This document provides a quick guide for setting up the container as well as Azure Batch job.

The Docker image shall use CFA Azure library to interact with Azure Blob service. It also needs Python 3.10+, Rust, Cargo and PIP package manager.

# Steps
1. Create a minimal `requirements.txt` file that will be installed in the Docker container:
  ```text
  azure-batch==14.0.0
  azure-mgmt-batch==17.1.0
  azure-storage-blob==12.17.0
  azure-containerregistry==1.2.0
  pandas
  cfa-azure @ git+https://github.com/CDCgov/cfa_azure.git
  ```

2. Create a `blob_config.toml` configuration file for connecting to Azure Blob service. Replace `azure_blob_test` with the desired container image name.
  ```text
  [Authentication]
  subscription_id="REPLACE_WITH_AZURE_SUBSCRIPTION_ID"
  resource_group="EXT-EDAV-CFA-PRD"
  user_assigned_identity="REPLACE_WITH_USER_ASSIGNED_ID"
  tenant_id="REPLACE_WITH_TENANT_ID"
  batch_application_id="REPLACE_WITH_BATCH_APP_ID"
  batch_object_id="REPLACE_WITH_BATCH_OBJECT_ID"
  sp_application_id="REPLACE_WITH_SERVICE_PRINCIPAL_APP_ID"
  vault_url="REPLACE_WITH_AZURE_VAULT_URL"
  vault_sp_secret_id="REPLACE_WITH_SECRET_ID"
  subnet_id="REPLACE_WITH_AZURE_SUBNET_ID"

  [Storage]
  storage_account_name="cfaazurebatchprd"
  storage_account_url="https://cfaazurebatchprd.blob.core.windows.net"

  [Container]
  container_registry_username="REPLACE_WITH_CONTAINER_REGISTRY_USERNAME"
  container_registry_url="REPLACE_WITH_CONTAINER_REGISTRY_URL"
  container_registry_password="*****"
  container_name="azure_blob_test:latest"
  container_image_name="azure_blob_test:latest"
  ```

3. Create a Python script `app.py` file that imports CFA Azure library, reads and writes data into Blob service:
  ```python
  import pandas as pd
  from datetime import datetime
  from cfa_azure.clients import AzureClient

  if __name__ == "__main__":
      # Initialize the AzureClient using configuration provideded
      client = AzureClient(config_path="./blob_config.toml", credential_method='sp')
      client.set_debugging(True)
      # Read the AZ.csv file from /input folder in input-test container into a Pandas dataframe
      data_stream = client.read_blob("input/AZ.csv", container="input-test")
      df = pd.read_csv(data_stream)
      # Generate a new file name
      dt = datetime.now()
      seq = int(dt.strftime("%Y%m%d%H%M%S"))
      blob_url = f"input/AZ_{seq}.csv"
      # Save the Pandas data frame to new file in input-test container
      client.write_blob(df.to_csv(index=False).encode('utf-8'), blob_url=blob_url, container='input-test')
  ```

4. Create a `Dockerfile` that packages all files together:
  ```text
  FROM python:3.10.17-slim-bullseye
  RUN apt-get update -y --fix-missing && apt-get install git -y
  WORKDIR /app
  COPY app.py /app
  COPY requirements.txt /app
  COPY blob_config.toml /app
  RUN pip install --upgrade pip
  RUN pip install --no-cache-dir -r requirements.txt

  CMD ["python", "app.py"]
  ```

5. Create a `batch_config.toml` file for configuring the connection to Azure Batch Service:
  ```text
  [Authentication]
  subscription_id="REPLACE_WITH_AZURE_SUBSCRIPTION_ID"
  resource_group="REPLACE_WITH_AZURE_RESOURCE_GROUP"
  user_assigned_identity="REPLACE_WITH_USER_ASSIGNED_ID"
  tenant_id="REPLACE_WITH_TENANT_ID"
  batch_application_id="REPLACE_WITH_BATCH_APP_ID"
  batch_object_id="REPLACE_WITH_BATCH_OBJECT_ID"
  sp_application_id="REPLACE_WITH_SERVICE_PRINCIPAL_APP_ID"
  vault_url="REPLACE_WITH_AZURE_VAULT_URL"
  vault_sp_secret_id="REPLACE_WITH_SECRET_ID"
  subnet_id="REPLACE_WITH_AZURE_SUBNET_ID"

  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  pool_vm_size="STANDARD_A2_V2"
  pool_name="REPLACE_WITH_POOL_NAME"
  scaling_mode="fixed"
  ```

6. Create a `client.py` Python file for orchestrating the batch job. The `repo_name` should match the `container_image_name` and `container_name` used in step 2.
  ```python
  from cfa_azure.clients import AzureClient

  client = AzureClient(config_path="./batch_config.toml")
  client.set_debugging(True)
  client.package_and_upload_dockerfile(
      registry_name="cfaprdbatchcr", repo_name="azure_blob_test", tag="latest"
  )
  job_id = "REPLACE_WITH_JOB_ID"
  pool_name = "REPLACE_WITH_POOL_NAME"
  client.set_pool_info(mode="fixed")
  client.create_pool(pool_name=pool_name)
  client.add_job(job_id=job_id)
  docker_cmd = "python app.py"
  task_1 = client.add_task(
      job_id=job_id,
      docker_cmd=docker_cmd,
      name_suffix="azure_blob_task",
      run_dependent_tasks_on_fail=False
  )
  print(f'tasks_1: {task_1}')
  client.monitor_job(job_id=job_id)
  ```

7. Run the previous script with `python client.py` to orchestrate the process
