from metaflow.metadata_provider.metadata import MetadataProvider


class LocalMetadataProvider(MetadataProvider):
    TYPE = "local"   # ✅ This makes --metadata local work
    
    def __init__(self, flow_name, environment, echo, *args, **kwargs):
        super().__init__(flow_name, environment, echo, *args, **kwargs)

    def _get_run_info(self, run_id):
        return {}

    def _register_run_id(self, run_id, tags=None, system_tags=None):
        pass

    def _register_step_name(self, run_id, step_name):
        pass

    def _register_task_id(self, run_id, step_name, task_id, attempt=0):
        pass

    def _register_metadata(self, metadata):
        print(f"Storing metadata locally: {metadata}")

    def _register_artifacts(self, artifacts):
        print(f"Registering artifacts locally: {artifacts}")

    def _log_artifacts(self, run_id, step_name, task_id, attempt, artifacts):
        pass

    def _log_metadata(self, run_id, step_name, task_id, attempt, metadata):
        pass

    def _artifact_needs_logging(self, name, value):
        return True
