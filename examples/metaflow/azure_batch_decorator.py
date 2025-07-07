from metaflow.decorators import StepDecorator
import json


class AzureBatchDecorator(StepDecorator):
    """
    A custom Metaflow StepDecorator that simulates submitting a step to Azure Batch.
    """

    name = "azure_batch"

    # Configuration options accepted via @azure_batch(...) or step_decorator field in JSON
    defaults = {
        "config": None
    }

    def __init__(self, attributes=None):
        # Properly call the base class with parsed attributes
        super(AzureBatchDecorator, self).__init__(attributes or {})
        self.config = None
        self.logger = None

    def step_init(self, flow, graph, node, decos, environment, flow_datastore, logger):
        self.logger = logger
        logger.info("[AzureBatchDecorator] step_init called.")

        config_path = self.attributes.get("config")
        if config_path:
            try:
                with open(config_path) as f:
                    self.config = json.load(f)
                logger.info(f"[AzureBatchDecorator] Loaded config: {self.config}")
            except Exception as e:
                logger.error(f"[AzureBatchDecorator] Failed to load config: {e}")
        else:
            logger.warning("[AzureBatchDecorator] No config file provided.")

    def step_task(self, step_name, input_paths, runtime_name, user_code_reqs):
        self.logger.info(f"[AzureBatchDecorator] Executing step_task for: {step_name}")
        self.logger.info(f"[AzureBatchDecorator] Simulating Azure Batch job execution...")
        self.logger.info(f"[AzureBatchDecorator] Config used: {self.config}")
        # You can simulate job submission here or print/log details
        return None

    def step_finalize(self, step_name, outputs, flow):
        self.logger.info(f"[AzureBatchDecorator] Finalizing step: {step_name}")
        return None
