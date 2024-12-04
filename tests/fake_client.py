# ruff: noqa: F403, F405

import json
from datetime import datetime, timedelta

import azure.batch.models as batchmodels

FAKE_ACCOUNT = "Test Account"
FAKE_AUTOSCALE_FORMULA = "some_formula"
FAKE_BATCH_POOL = "test_pool"
FAKE_BLOBS = ["some_path/fake_blob_1.txt", "some_path/fake_blob_2.csv"]
FAKE_BLOB_CONTENT = "Test Blob Content"
FAKE_CONTAINER_IMAGE = "Test Container Image"
FAKE_CONTAINER_REGISTRY = "Test Container Registry"
FAKE_FOLDER = "/test_folder"
FAKE_FOLDER_CONTENTS = [
    f"{FAKE_FOLDER}/test_file.csv",
    f"{FAKE_FOLDER}/test_file.txt",
]
FAKE_INPUT_CONTAINER = "test_input_container"
FAKE_OUTPUT_CONTAINER = "test_output_container"
FAKE_POOL_SIZE = 10
FAKE_RESOURCE_GROUP = "Test Resource Group"
FAKE_SECRET = "fake_secret"  # pragma: allowlist secret
FAKE_TAGS = ["fake_tag_1", "fake_tag_2", "latest"]

FAKE_YAML_CONTENT = {
    "baseScenario": {"r0": 10},
    "outputDirectory": "some_directory",
}
FAKE_CONFIG_MINIMAL = {
    "Authentication": {
        "resource_group": FAKE_RESOURCE_GROUP,
        "subscription_id": "Test Subscription ID",
        "subnet_id": "Test Subnet ID",
        "tenant_id": "Test Tenant ID",
        "sp_application_id": "Some App ID",
        "batch_application_id": "Some Batch App ID",
        "batch_object_id": "Some Batch Object ID",
        "user_assigned_identity": "Test User Identity",
        "vault_sp_secret_id": "Test Vault Service Principal",
        "vault_url": "Test Vault URL",
    },
    "Batch": {
        "batch_account_name": FAKE_ACCOUNT,
        "batch_service_url": "Test Batch Service URL",
        "pool_vm_size": 10,
    },
    "Container": {
        "container_registry_password": "Test ACR Password",  # pragma: allowlist secret
        "container_registry_username": "Test ACR Username",
    },
    "Storage": {
        "storage_account_name": "Test Storage Account",
        "storage_account_url": "Test Storage Account URL",
    },
}

FAKE_CONFIG = {
    "Authentication": {
        "application_id": "Test Application ID",
        "batch_application_id": "Test Batch Application ID",
        "batch_object_id": "Test Batch Object ID",
        "client_id": "Test Client ID",
        "principal_id": "Test Principal ID",
        "resource_group": FAKE_RESOURCE_GROUP,
        "subscription_id": "Test Subscription ID",
        "subnet_id": "Test Subnet ID",
        "tenant_id": "Test Tenant ID",
        "user_assigned_identity": "Test User Identity",
        "vault_sp_secret_id": "Test Vault Service Principal",
        "vault_url": "Test Vault URL",
    },
    "Batch": {
        "batch_account_name": FAKE_ACCOUNT,
        "batch_service_url": "Test Batch Service URL",
        "pool_id": FAKE_BATCH_POOL,
        "pool_vm_size": 10,
    },
    "Container": {
        "container_account_name": "Test Account",
        "container_image_name": FAKE_CONTAINER_IMAGE,
        "container_name": FAKE_INPUT_CONTAINER,
        "container_registry_password": "Test ACR Password",  # pragma: allowlist secret
        "container_registry_url": FAKE_CONTAINER_REGISTRY,
        "container_registry_username": "Test ACR Username",
    },
    "Storage": {
        "storage_account_name": "Test Storage Account",
        "storage_account_url": "Test Storage Account URL",
    },
}

FAKE_POOL_INFO = {
    "deployment_configuration": {
        "virtual_machine_configuration": {
            "container_configuration": {
                "container_image_names": [FAKE_CONTAINER_IMAGE]
            }
        }
    },
    "resize_operation_status": {"resize_timeout": 10},
    "vm_size": 20,
    "mount_configuration": {},
}


class FakeClient:
    class FakeBatchJob:
        def delete(self, *args):
            return True

        def add(self, job):
            return True

    class FakeTag:
        def __init__(self, tag):
            self.name = tag

    class FakeBlob:
        def __init__(self, name):
            self.name = name

        def write(self, bytes):
            return True

        def readall(self):
            return bytes(FAKE_BLOB_CONTENT, "utf-8")

    class FakeTask:
        @property
        def state(self):
            return batchmodels.TaskState.completed

        def add(self, job_id, task):
            return True

        def as_dict(self):
            return {"execution_info": {"result": "success"}}

        def list(self, job_id):
            return [FakeClient.FakeTask()]

    class FakeComputeNode:
        def __init__(self, state: str):
            self.state = state

    class FakeComputeNodeList:
        def list(self, pool_id, compute_node_list_options=None) -> list:
            if compute_node_list_options:
                return [
                    FakeClient.FakeComputeNode("running"),
                    FakeClient.FakeComputeNode("running"),
                ]
            return [
                FakeClient.FakeComputeNode("running"),
                FakeClient.FakeComputeNode("idle"),
                FakeClient.FakeComputeNode("running"),
                FakeClient.FakeComputeNode("unusable"),
            ]

    class FakeContainerClient:
        def exists(self):
            return False

        def create_container(self):
            return True

        def list_blobs(self, name_starts_with=None):
            return [FakeClient.FakeBlob(f) for f in FAKE_BLOBS]

    class FakeSecretClient:
        class FakeSecret:
            @property
            def value(self):
                return FAKE_SECRET

        def __init__(self, vault_url, credential):
            print("reached here BB")
            self.vault_url = vault_url
            self.credential = credential

        def get_secret(self, vault_sp_secret_id=None):
            print("reached here BBB")
            return self.FakeSecret()

    class FakePool:
        class FakePoolInfo:
            class FakeScaleSettings:
                @property
                def auto_scale(self):
                    return "fixed"

                def as_dict(self):
                    return FAKE_POOL_INFO

            class FakeDeploymentConfig:
                class VMConfiguration:
                    class ContainerConfig:
                        class FakeContainerRegistry:
                            @property
                            def registry_server(self):
                                return "registry_server"

                            @property
                            def user_name(self):
                                return "user_name"

                        @property
                        def container_image_names(self):
                            return [FAKE_CONTAINER_IMAGE]

                        @property
                        def container_registries(self):
                            return [self.FakeContainerRegistry()]

                    @property
                    def container_configuration(self):
                        return self.ContainerConfig()

                @property
                def virtual_machine_configuration(self):
                    return self.VMConfiguration()

            def get_past_time(self, elapsed_minutes: int):
                return (
                    datetime.now() - timedelta(minutes=elapsed_minutes)
                ).strftime("%d/%m/%y %H:%M")

            def as_dict(self):
                return FAKE_POOL_INFO

            @property
            def deployment_configuration(self):
                return self.FakeDeploymentConfig()

            @property
            def creation_time(self):
                return self.get_past_time(10)

            @property
            def last_modified(self):
                return self.get_past_time(15)

            @property
            def vm_size(self):
                return FAKE_POOL_SIZE

            def get(self):
                return True

        def get(self, resource_group_name, account_name, pool_name):
            return self.FakePoolInfo()

        def create(
            self, resource_group_name, account_name, pool_name, parameters
        ):
            return dict2obj({"name": pool_name})

    @property
    def job(self) -> FakeBatchJob:
        return self.FakeBatchJob()

    @property
    def pool(self) -> FakePool:
        return self.FakePool()

    @property
    def task(self) -> FakeTask:
        return self.FakeTask()

    @property
    def compute_node(self) -> FakeComputeNodeList:
        return self.FakeComputeNodeList()

    def get_container_client(self, container):
        return self.FakeContainerClient()

    def download_blob(self, blob):
        return self.FakeBlob("blob_name")

    def ping(self):
        return True

    def done(self):
        return True


class FakeContainerRegistryClient:
    def __init__(self, endpoint, credential, audience):
        self.endpoint = endpoint
        self.credential = credential
        self.audience = audience

    def list_tag_properties(self, repo_name):
        return [FakeClient.FakeTag(t) for t in FAKE_TAGS]

    def get_tag_properties(self, repo_name, tag_name):
        return FakeClient.FakeTag(tag_name)


class obj:
    def __init__(self, dict1):
        self.__dict__.update(dict1)


def dict2obj(dict1):
    # using json.loads method and passing json.dumps
    # method and custom object hook as arguments
    return json.loads(json.dumps(dict1), object_hook=obj)
