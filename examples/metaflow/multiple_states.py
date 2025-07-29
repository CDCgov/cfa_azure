from metaflow import FlowSpec, step
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
plugins_folder = os.path.join(current_dir, "custom_metaflow", "plugins", "decorators")
if plugins_folder not in sys.path:
    sys.path.insert(0, plugins_folder)

from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import CFAAzureBatchDecorator

class MyFlow(FlowSpec):
    @step
    def start(self):
        print("Starting the flow...")
        self.all_states = []
        with open('states.txt', 'r') as file:
            self.all_states = file.read().splitlines()
        self.next(self.process_state, foreach='all_states')

    @step
    def process_state(self):
        # Dynamically apply the decorator
        decorator = CFAAzureBatchDecorator(config_file="client_config_states.toml", docker_command=f'echo {self.input}')
        decorator(self._process_state)()
        self.next(self.join)

    def _process_state(self):
        print(f"Running the _process_state step in Azure Batch for {self.input}...")

    @step
    def join(self, inputs):
        print("Flow joined.")
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()