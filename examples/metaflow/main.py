from metaflow import FlowSpec, step
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
plugins_folder = os.path.join(current_dir, "custom_metaflow", "plugins", "decorators")
if plugins_folder not in sys.path:
    sys.path.insert(0, plugins_folder)

from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import CFAAzureBatchDecorator

cfa_azure_batch = CFAAzureBatchDecorator(config_file="client_config.toml")

class MyFlow(FlowSpec):
    @step
    def start(self):
        print("Starting the flow...")
        self.next(self.foo)

    @step
    @cfa_azure_batch
    def foo(self):
        print("Running the foo step in Azure Batch...")

        from cfa_azure.clients import AzureClient
        import pandas as pd
        from datetime import datetime

        client = AzureClient(config_path="./storage_config.toml", credential_method='sp')
        data_stream = client.read_blob("input/AZ.csv", container="input-test")
        df = pd.read_csv(data_stream)
        dt = datetime.now()
        seq = int(dt.strftime("%Y%m%d%H%M%S"))
        blob_url = f"input/AZ_{seq}.csv"
        client.write_blob(df.to_csv(index=False).encode('utf-8'), blob_url=blob_url, container='input-test')
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()