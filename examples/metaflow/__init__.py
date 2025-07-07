import os
import sys

# 💡 Add the root of the project to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


# --- PLUGIN DEFINITIONS ---

# 👇 Your custom metadata and step decorator
from examples.metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from examples.metaflow.azure_batch_decorator import AzureBatchDecorator

# 👇 Metaflow plugin loader
from metaflow.plugins import resolve_plugins

# ✅ Expose custom plugins using expected global names
ENABLED_METADATA = {
    "local": LocalMetadataProvider
}

ENABLED_STEP_DECORATORS = {
    "azure_batch": AzureBatchDecorator
}

# --- DYNAMIC RESOLUTION ---

STEP_DECORATORS = list(ENABLED_STEP_DECORATORS.values())
FLOW_DECORATORS = []
ENVIRONMENTS = []
DATASTORES = []
DATACLIENTS = []
SIDECARS = []
LOGGING_SIDECARS = []
MONITOR_SIDECARS = []

# ✅ Must be resolved AFTER ENABLED_METADATA is set
METADATA_PROVIDERS = list(ENABLED_METADATA.values())

# 👇 Combine sidecars
SIDECARS += LOGGING_SIDECARS + MONITOR_SIDECARS
