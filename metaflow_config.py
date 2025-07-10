# examples/metaflow/metaflow_config.py

PLUGINS = [
    'azure_batch_decorator'
]

METAFLOW_DEFAULT_METADATA = 'local'
METAFLOW_DATASTORE_SYSROOT_LOCAL = '/tmp/metaflow'

# Your custom plugin path should exist
STEP_DECORATORS = {
    'azurebatch': 'examples.metaflow.plugins.azure.batch.azure_batch_decorator.AzureBatchDecorator'
}
