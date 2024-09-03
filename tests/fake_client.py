from datetime import datetime, timedelta

FAKE_POOL_SIZE = 10 

class FakeClient:
    class FakeBatchJob:
        def delete(self, *args):
            return True

    class FakePool:
        class FakePoolInfo:
            @property
            def creation_time(self):
                return (datetime.now() - timedelta(minutes=10)).strftime("%d/%m/%y %H:%M")
            
            @property
            def last_modified(self):
                return (datetime.now() - timedelta(minutes=10)).strftime("%d/%m/%y %H:%M")

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
