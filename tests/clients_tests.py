import unittest
from unittest.mock import patch, MagicMock, call
import cfa_azure.clients
import cfa_azure.helpers
from tests.fake_client import *

class TestClients(unittest.TestCase):

    @patch("cfa_azure.clients.logger")
    @patch("cfa_azure.helpers.read_config", MagicMock(return_value=FAKE_CONFIG))
    @patch("cfa_azure.helpers.check_config_req", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_blob_service_client", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_mgmt_client", MagicMock(return_value=FakeClient()))
    @patch("cfa_azure.helpers.get_batch_service_client", MagicMock(return_value=FakeClient()))
    def setUp(self, mock_logger):
        config_path = "some_path"
        self.azure_client = cfa_azure.clients.AzureClient(config_path)
        mock_logger.info.assert_called_with("Client initialized! Happy coding!")

    @patch("cfa_azure.clients.logger")
    def test_set_pool_info(self, mock_logger):
        self.azure_client.set_pool_info(
            mode="fixed",
            max_autoscale_nodes=3,
            autoscale_formula_path=None,
            timeout=60,
            dedicated_nodes=1,
            low_priority_nodes=0,
            cache_blobfuse=True
        )
        mock_logger.debug.assert_called_with("pool parameters generated")
    
    @patch("cfa_azure.helpers.check_azure_container_exists", MagicMock(return_value=FAKE_CONTAINER_IMAGE))    
    def test_add_task(self):
        task_list = self.azure_client.add_task(
            "test_job_id",
            docker_cmd=["some", "docker", "command"], 
            use_uploaded_files=False, 
            input_files=None, 
            depends_on=None, 
            container=FAKE_INPUT_CONTAINER
        )
        self.assertIsNotNone(task_list)
