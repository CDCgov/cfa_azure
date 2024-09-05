from datetime import datetime, timedelta

FAKE_ACCOUNT            = 'Test Account'
FAKE_AUTOSCALE_FORMULA  = 'some_formula'
FAKE_BATCH_POOL         = 'test_pool'
FAKE_INPUT_CONTAINER    = 'test_input_container'
FAKE_OUTPUT_CONTAINER   = 'test_output_container'
FAKE_POOL_SIZE          = 10 
FAKE_RESOURCE_GROUP     = 'Test Resource Group'
FAKE_SECRET             = "fake_secret"

FAKE_CONFIG = {
    'Authentication': {
        'resource_group': FAKE_RESOURCE_GROUP,
        'user_assigned_identity': 'Test User Identity',
        'client_id': 'Test Client ID',
        'principal_id': 'Test Principal ID',
        'subnet_id': 'Test Subnet ID'

    },
    'Batch': {
        'batch_account_name': FAKE_ACCOUNT,
        'pool_vm_size': 10,
        'pool_id': FAKE_BATCH_POOL
    },
    'Container': {
        'container_name': FAKE_INPUT_CONTAINER,
        'container_image_name': 'Test Container Image',
        'container_account_name': 'Test Account',
        'container_registry_url': 'Test ACR Url',
        'container_registry_username': 'Test ACR Username',
        'container_registry_password': 'Test ACR Password'
    },
    'Storage': {
        'storage_account_name': 'Test Storage Account'
    }
}


class FakeClient:
    class FakeBatchJob:
        def delete(self, *args):
            return True

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
            

        def get(self, resource_group_name, account_name, pool_name):
            return self.FakePoolInfo()
        
    @property
    def pool(self) -> FakePool:
        return self.FakePool()    

    @property
    def job(self) -> FakeBatchJob:
        return self.FakeBatchJob()
        
    def get_container_client(self, container):
        return self.FakeContainerClient()
