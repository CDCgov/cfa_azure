import unittest
import json
import os
import sys
from unittest.mock import patch

# Add project root to PYTHONPATH for local execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from examples.metaflow.azure_batch_decorator import AzureBatchDecorator

class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(("INFO", msg))

    def warning(self, msg):
        self.messages.append(("WARNING", msg))

    def error(self, msg):
        self.messages.append(("ERROR", msg))


class TestAzureBatchDecoratorEnhanced(unittest.TestCase):

    def setUp(self):
        self.valid_config_path = "examples/metaflow/configs/sample_config.json"
        self.invalid_config_path = "examples/metaflow/configs/does_not_exist.json"
        self.logger = DummyLogger()

        self.assertTrue(os.path.exists(self.valid_config_path), "Sample config file is missing!")

    def test_step_init_with_valid_config(self):
        decorator = AzureBatchDecorator({"config": self.valid_config_path})
        decorator.step_init(flow=None, graph=None, node=None, decos=None,
                            environment=None, flow_datastore=None, logger=self.logger)

        self.assertIsNotNone(decorator.config)
        self.assertIn("pool_id", decorator.config)
        self.assertEqual(decorator.config["pool_id"], "cfa-batch-pool")
        self.assertTrue(any("Loaded config" in msg for lvl, msg in self.logger.messages))

    def test_step_task_logging(self):
        decorator = AzureBatchDecorator({"config": self.valid_config_path})
        decorator.logger = self.logger
        decorator.config = {
            "pool_id": "dummy",
            "vm_size": "dummy",
            "docker_image": "dummy",
            "command": "echo test"
        }

        decorator.step_task("azure_step", [], "python3", None)
        self.assertTrue(any("Simulating Azure Batch job execution" in msg for lvl, msg in self.logger.messages))

    def test_step_init_with_invalid_config(self):
        decorator = AzureBatchDecorator({"config": self.invalid_config_path})
        decorator.step_init(flow=None, graph=None, node=None, decos=None,
                            environment=None, flow_datastore=None, logger=self.logger)

        self.assertIsNone(decorator.config)
        self.assertTrue(any("Failed to load config" in msg for lvl, msg in self.logger.messages))

    @patch("builtins.print")
    def test_simulated_batch_submission(self, mock_print):
        decorator = AzureBatchDecorator({"config": self.valid_config_path})
        decorator.logger = self.logger
        decorator.config = {
            "pool_id": "mockpool",
            "vm_size": "Standard",
            "docker_image": "mock:image",
            "command": "echo mock"
        }

        decorator.step_task("mock_step", [], "python3", None)
        mock_print.assert_not_called()  # Confirm no print inside step_task

if __name__ == '__main__':
    unittest.main()
