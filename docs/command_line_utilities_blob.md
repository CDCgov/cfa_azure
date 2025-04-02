# Command-line Utilities for Azure Blob Storage
## Created by Fawad Rafi (Peraton) for CFA

# Outline
Users can interact with Azure Blob Storage using command-line utilities without writing any Python code. These utilites can be invoked from Linux shell script and impersonate the credentials of specified Azure Service Principal.

Currently the following utilities are available:

1. copy_blob
2. write_blob

# Prerequisites and installation
Python 3.9+ is a prerequisite for both utilities and must be installed in the VAP.

1. First launch a WSL instance and install the PIPX package manager (if it is not already installed)

```shell
sudo apt update
sudo apt install pipx -y
pipx ensurepath
```

2. Install the CFA Azure library and Poetry using PIPX (if it is not already installed)
```shell
pipx install git+https://github.com/CDCgov/cfa_azure.git
```

# download_blob
Use this command line utility to download a file from specified Azure Blob container to a local file in VAP.

Example: Download `myfile.pdf` file from `input/files` subfolder in `input-test` container of Azure blob storage account `cfaazurebatchprd` to `/Downloads/myfile.pdf` file in VAP:
```shell
download_blob --account cfaazurebatchprd --container input-test --localpath /Downloads/myfile.pdf --blobpath input/files/myfile.pdf
```

# upload_blob
Use this utility to upload a local file from VAP to specified Azure Blob container.

Example: Upload `myfile.pdf` file from `/Downloads` folder in VAP to `/input/files/myfile_v2.pdf` path inside `input-test` container of Azure blob storage account `cfaazurebatchprd`
```shell
 upload_blob --account cfaazurebatchprd --container input-test --localpath /Downloads/myfile.pdf --blobpath input/files/myfile_v2.pdf
```
