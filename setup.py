from setuptools import find_packages, setup

setup(
    name="cfa_azure",
    version="1.0.8",
    description="module for use with Azure and Azure Batch",
    packages=find_packages(exclude=["tests", "venv"]),
    author="Ryan Raasch",
    author_email="xng3@cdc.gov",
    install_requires=[
        "azure-identity==1.16.1",
        "azure-keyvault==4.2.0",
        "azure-batch==14.0.0",
        "azure-mgmt-batch==17.1.0",
        "azure-storage-blob==12.17.0",
        "azure-containerregistry==1.2.0",
        "cryptography>=43.0.1"
        "toml==0.10.2",
        "pandas",
        "pathlib",
        "docker",
        "pyyaml",
    ],
)
