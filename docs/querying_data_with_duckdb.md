# Querying Azure Blobs with DuckDB
## Created by Fawad Rafi (Peraton) for CFA

# Outline
DuckDB is a free, open-source, embedded, in-process, columnar database used for Online Analytical Processing (OLAP). It is optimized for reading and computing on the columns efficiently. It runs on Linux, macOS, Windows, Android, iOS and all popular hardware architectures. It has idiomatic client APIs for major programming languages. DuckDB is extensible by third-party features such as new data types, functions, file formats and new SQL syntax. User contributions are available as community extensions. It has zero external dependencies and runs in-process in its host application or as a single binary.

# Install DuckDB on VAP
Follow these steps to install DuckDB:

1. On Linux-based VAPs, you can install directly on bash shell. On Windows-based VAPs, you need to launch WSL session. 
2. Download and install DuckDB on Linux:
   ```bash
   curl https://install.duckdb.org | sh
   ```
3. Configure SSL Certificate for DuckDB process:
   ```bash
   ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
   ```
4. Run the DuckDB interactive console:
   ```bash
   /root/.duckdb/cli/latest/duckdb
   ```

# Connect DuckDB with Azure Blob Storage

There are options for connecting DuckDB to Azure Blob Storage: DuckDB Azure extension or Blobfuse. 

## DuckDB Azure Extension

Use this option if you want to read blob data quickly. The extension authenticates to Azure Blob Storage using Azure Service Principal and reads a blob from specified container into a DuckDB table. However, all updates, insertions and deletes are local. DuckDB Azure extension does not support writes to Blob Storage. 

Follow these steps to connect through Azure extension:

1. Obtain the client secret for Service Principal from Azure Key Vault:
```python
from cfa_azure.helpers import get_sp_secret
from azure.identity import ManagedIdentityCredential
    auth_config = {
    "Authentication": {
        "vault_url": "https://cfa-predict.vault.azure.net/",
        "vault_sp_secret_id": "REPLACE WITH SECRET NAME"
        }
    }

client_secret = get_sp_secret(auth_config, ManagedIdentityCredential())
print(client_secret)
```
2. Inside DuckDB interactive console, install Azure extension and confirm the extension was installed:
```shell
INSTALL azure;
LOAD azure;
FROM duckdb_extensions();
```
3. Create connection to the Azure Blob Storage container
```shell
CREATE SECRET azure_spn (
   TYPE azure,
   PROVIDER service_principal,
   TENANT_ID 'REPLACE WITH TENANT ID',
   CLIENT_ID 'REPLACE WITH SERVICE PRINCIPAL CLIENT ID',
   CLIENT_SECRET 'REPLACE WITH CLIENT SECRET FROM STEP 1',
   ACCOUNT_NAME 'REPLACE WITH AZURE STORAGE ACCOUNT NAME'
);
```
4. Create table (e.g. Arizona_Towns) with data from Blob (e.g. `az://input-test/input/AZ_03072025_a.csv`):
```shell
CREATE TABLE IF NOT EXISTS Arizona_Towns 
AS SELECT * FROM 'az://CONTAINER_NAME/PATH_TO_BLOB';        

SELECT * FROM Arizona_Towns;
```

## Blobfuse

The Linux Blobfuse v2 library mounts a Blob container to local Linux folder in the VAP using an Azure Service Principal. With this option, you can read structured data into a DuckDB table and write DuckDB tables into Blob Storage using any delimiter. 

Follow these steps to connect through Azure extension:

1. Obtain the client secret for Service Principal from Azure Key Vault:
```python
from cfa_azure.helpers import get_sp_secret
from azure.identity import ManagedIdentityCredential
    auth_config = {
    "Authentication": {
        "vault_url": "https://cfa-predict.vault.azure.net/",
        "vault_sp_secret_id": "REPLACE WITH SECRET NAME"
        }
    }

client_secret = get_sp_secret(auth_config, ManagedIdentityCredential())
print(client_secret)
```
2. Install Blobfuse v2 and dependencies:
```bash
wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update
sudo apt-get install libfuse2 blobfuse2
```
3. Create a connection.yaml with connection settings for Azure Blob and Service Principal
```bash
cat << EOF > connection.yaml
file_cache:
  path: local-cache


azstorage:
  type: block
  account-name: REPLACE WITH AZURE STORAGE ACCOUNT NAME
  mode: spn
  container: REPLACE WITH CONTAINER NAME WITHIN AZURE STORAGE ACCOUNT
  tenantid: REPLACE WITH TENANT ID
  clientid: REPLACE WITH SERVICE PRINCIPAL CLIENT ID
  clientsecret: REPLACE WITH CLIENT SECRET FROM STEP 1
EOF
```
4. Create a temporary mount location (e.g. `my_temporary_blob_folder`) and destination folder (e.g. `my_blob`) in VAP for Azure Blob Container:
```bash
sudo mkdir /mnt/my_temporary_blob_folder -p
sudo chown root /mnt/my_temporary_blob_folder
mkdir ~/my_blob
```
5. Mount the Azure Blob Storage container to destination folder in VAP folder:
```bash
sudo blobfuse2 ~/my_blob --tmp-path=/mnt/my_temporary_blob_folder --config-file=./connection.yml -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120 -o allow_other
```
6. If blob was successfully mounted, you can list contents of the destination folder. These should match the contents of Azure Blob:
```bash
ls ~/my_blob
```
7. Read structured files stored on Blob into a table inside the DuckDB interactive shell:
```shell
CREATE TABLE IF NOT EXISTS Arizona_Towns
AS SELECT * FROM '~/my_blob/input/AZ.csv';

SELECT * FROM Arizona_Towns;
```
8. Add or delete records from the DuckDB table and copy the updated table to locally-mounted Blob folder:
```shell
INSERT INTO Arizona_Towns VALUES (5, 'Scottsdale', 90);

COPY Arizona_Towns TO '~/my_blob/input/AZ_03072025_a.csv' (HEADER, DELIMITER ',');
```
9. Open the file stored in Azure Blob Container and confirm it contains the updates.
10. Unmount the Blob Storage if you don't need it any more:
```bash
sudo blobfuse2 unmount ~/my_blob
```