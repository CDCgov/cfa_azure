import os
import sys

# Add root of project to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Disable Metaflow plugin system to prevent unrelated plugin loading errors
os.environ["METAFLOW_DEFAULT_DECORATORS"] = "none"
os.environ["METAFLOW_DEFAULT_FLOW_DECORATORS"] = "none"
os.environ["METAFLOW_PLUGIN_PATHS"] = "examples/metaflow"
os.environ["METAFLOW_EXTENSIONS_DIR"] = os.path.abspath("examples/metaflow")
# os.environ["METAFLOW_METADATA"] = "local"

# Prevent AWS-specific plugins from loading
os.environ["METAFLOW_ALLOW_AWS_STEP_FUNCTIONS"] = "false"
os.environ["METAFLOW_ALLOW_AWS_BATCH"] = "false"
os.environ["METAFLOW_ALLOW_S3"] = "false"

# Optional: unset METAFLOW_METADATA if still present
os.environ.pop("METAFLOW_METADATA", None)

# Log resolved paths
print("✅ Using Metaflow config: Inside examples/metaflow/tests/test_decorator_flow.py")
# print("🚀 PYTHONPATH:", sys.path)
print("📂 METAFLOW_PLUGIN_PATHS:", os.environ["METAFLOW_PLUGIN_PATHS"])
print("📂 METAFLOW_EXTENSIONS_DIR:", os.environ["METAFLOW_EXTENSIONS_DIR"])
# print("🧩 METAFLOW_METADATA:", os.environ["METAFLOW_METADATA"])

# Import Metaflow
from metaflow import FlowSpec, step

# Define your flow
class TestAzureFlow(FlowSpec):

    @step
    def start(self):
        print("✅ Step: start")
        self.next(self.azure_step)

    @step
    def azure_step(self):
        print("🚀 Step: azure_step (simulating Azure Batch job)")
        self.next(self.end)

    @step
    def end(self):
        print("✅ Step: end - Flow complete")

# Run the flow
if __name__ == '__main__':
    TestAzureFlow()
