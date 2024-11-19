import json
import unittest
from unittest.mock import MagicMock, call, mock_open, patch

import docker
from azure.core.exceptions import HttpResponseError
from callee import Contains

import cfa_azure.clients
import cfa_azure.helpers
from tests.fake_client import *


class TestHelpers2(unittest.TestCase):
    def foo():
        return True

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.helpers.read_config", MagicMock(return_value=FAKE_CONFIG)
    )
    @patch("cfa_azure.helpers.check_config_req", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch(
        "cfa_azure.helpers.get_blob_service_client",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.helpers.get_batch_mgmt_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    def setUp(self, mock_logger):
        config_path = "some_path"
        self.azure_client = cfa_azure.clients.AzureClient(config_path)
        mock_logger.info.assert_called_with(
            "Client initialized! Happy coding!"
        )

    # set_azure_container

    # upload_files
