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
        self.next(self.perform_remote_task)

    @step
    @cfa_azure_batch
    def perform_remote_task(self):
        print("Running the perform_remote_task step in Azure Batch...")

        from cfa_azure.clients import AzureClient
        import pandas as pd
        client = AzureClient(config_path="./storage_config.toml", credential_method='sp')
        data_stream = client.read_blob("input/AZ.csv", container="input-test")
        df = pd.read_csv(data_stream)
        print(df.head(1))
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()