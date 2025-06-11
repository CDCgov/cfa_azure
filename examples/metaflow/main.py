from metaflow import FlowSpec, step
from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import CFAAzureBatchDecorator


class MyFlow(FlowSpec):
    @step
    def start(self):
        print("Starting the flow...")
        self.next(self.foo)

    @step
    @CFAAzureBatchDecorator(config_file="client_config.toml")
    def foo(self):
        print("Running the foo step in Azure Batch...")
        #from cfa_azure.clients import AzureClient

        #client = AzureClient(config_path="./storage_config.toml", credential_method='sp')
        #data_stream = client.read_blob("input/AZ.csv", container="input-test")
        #df = pd.read_csv(data_stream)
        #dt = datetime.now()
        #seq = int(dt.strftime("%Y%m%d%H%M%S"))
        #blob_url = f"input/AZ_{seq}.csv"
        #client.write_blob(df.to_csv(index=False).encode('utf-8'), blob_url=blob_url, container='input-test')
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()