from .local import LocalMetadataProvider

# Required for Metaflow plugin discovery
__mf_metadata_providers__ = {
    'local': LocalMetadataProvider
}
