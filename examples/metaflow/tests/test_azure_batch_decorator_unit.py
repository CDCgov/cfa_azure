import unittest
import json
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from examples.metaflow.azure_batch_decorator import AzureBatchDecorator

class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        print("INFO:", msg)
        self.messages.append(("INFO", msg))

    def warning(self, msg):
        print("WARNING:", msg)
        self.messages.append(("WARNING", msg))

    def error(self, msg):
        print("ERROR:", msg)
        self.messages.append(("ERROR", msg))


class TestAzureBatchDecoratorUnit(unittest.TestCase):

    def setUp(self):
        self.config_path = "examples/metaflow/configs/sample_config.json"
        self.logger = DummyLogger()

        # Sanity check config file exists
        self.assertTrue(os.path.exists(self.config_path), "Sample config file is missing!")

    def test_step_init_loads_config(self):
        decorator = AzureBatchDecorator({"config": self.config_path})
        decorator.step_init(
            flow=None,
            graph=None,
            node=None,
            decos=None,
            environment=None,
            flow_datastore=None,
            logger=self.logger
        )

        self.assertIsNotNone(decorator.config)
        self.assertIn("pool_id", decorator.config)
        self.assertEqual(decorator.config["pool_id"], "cfa-batch-pool")

    def test_step_task_logs(self):
        decorator = AzureBatchDecorator({"config": self.config_path})
        decorator.logger = self.logger
        decorator.config = {
            "pool_id": "dummy",
            "vm_size": "dummy",
            "docker_image": "dummy",
            "command": "echo test"
        }

        decorator.step_task("azure_step", [], "python3", None)
        self.assertTrue(any("Simulating Azure Batch job execution" in msg for lvl, msg in self.logger.messages))


if __name__ == '__main__':
    unittest.main()
