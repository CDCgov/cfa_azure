from datetime import datetime, timedelta
import azure.batch.models as batchmodels

FAKE_ACCOUNT            = 'Test Account'
FAKE_AUTOSCALE_FORMULA  = 'some_formula'
FAKE_BATCH_POOL         = 'test_pool'
FAKE_CONTAINER_IMAGE    = 'Test Container Image'
FAKE_FOLDER             = '/test_folder'
FAKE_FOLDER_CONTENTS    =  [f'{FAKE_FOLDER}/test_file.csv', f'{FAKE_FOLDER}/test_file.txt']
FAKE_INPUT_CONTAINER    = 'test_input_container'
FAKE_OUTPUT_CONTAINER   = 'test_output_container'
FAKE_POOL_SIZE          = 10
FAKE_RESOURCE_GROUP     = 'Test Resource Group'
FAKE_SECRET             = "fake_secret"

FAKE_CONFIG = {
    'Authentication': {
        'application_id': 'Test Application ID',
        'client_id': 'Test Client ID',
        'principal_id': 'Test Principal ID',
        'resource_group': FAKE_RESOURCE_GROUP,
        'subscription_id': 'Test Subscription ID',
        'subnet_id': 'Test Subnet ID',
        'tenant_id': 'Test Tenant ID',
        'user_assigned_identity': 'Test User Identity',
        'vault_sp_secret_id': 'Test Vault Service Principal',
        'vault_url': 'Test Vault URL'
    },
    'Batch': {
        'batch_account_name': FAKE_ACCOUNT,
        'batch_service_url': 'Test Batch Service URL',
        'pool_id': FAKE_BATCH_POOL,
        'pool_vm_size': 10
    },
    'Container': {
        'container_account_name': 'Test Account',
        'container_image_name': FAKE_CONTAINER_IMAGE,
        'container_name': FAKE_INPUT_CONTAINER,
        'container_registry_password': 'Test ACR Password',
        'container_registry_url': 'Test ACR Url',
        'container_registry_username': 'Test ACR Username'
    },
    'Storage': {
        'storage_account_name': 'Test Storage Account',
        'storage_account_url': 'Test Storage Account URL'
    }
}

FAKE_POOL_INFO = {
    "resize_operation_status": {
        "resize_timeout": 10
    }
}

class FakeClient:
    class FakeBatchJob:
        def delete(self, *args):
            return True

    class FakeTask:
        @property
        def state(self):
            return batchmodels.TaskState.completed

        def add(self, job_id, task):
            return True

        def as_dict(self):
            return {
                "execution_info": {
                    "result": "success"
                }
            } 

        def list(self, job_id):
            return [FakeClient.FakeTask()]

    class FakeContainerClient:
        def exists(self):
            return False
            
        def create_container(self):
            return True

    class FakePool:
        class FakePoolInfo:
            def get_past_time(self, elapsed_minutes:int):
                return (datetime.now() - timedelta(minutes=elapsed_minutes)).strftime("%d/%m/%y %H:%M")
            
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
        
    @property
    def job(self) -> FakeBatchJob:
        return self.FakeBatchJob()

    @property
    def pool(self) -> FakePool:
        return self.FakePool()
        
    @property
    def task(self) -> FakeTask:
        return self.FakeTask()

    def get_container_client(self, container):
        return self.FakeContainerClient()
