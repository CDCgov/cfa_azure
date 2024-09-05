import unittest
from unittest.mock import patch, MagicMock, call
import cfa_azure.helpers
from tests.fake_client import *
import logging

class TestHelpers(unittest.TestCase):

    @patch("cfa_azure.helpers.logger")
    @patch("toml.load", MagicMock(return_value=FAKE_CONFIG))
    def test_read_config(self, mock_logger):
        config_path = "some_path"
        config = cfa_azure.helpers.read_config(config_path)
        mock_logger.debug.assert_called_with("Configuration file loaded.")
        self.assertEqual(config, FAKE_CONFIG)

    @patch("cfa_azure.helpers.logger")
    @patch("tests.fake_client.FakeClient.FakeContainerClient.exists", MagicMock(return_value=False))
    def test_create_container(self, mock_logger):
        mock_client = FakeClient()
        container_name = "some_container"
        cfa_azure.helpers.create_container(container_name, mock_client)
        mock_logger.debug.assert_called_with(f"Container [{container_name}] created successfully.")

    @patch("cfa_azure.helpers.logger")
    @patch("tests.fake_client.FakeClient.FakeContainerClient.exists", MagicMock(return_value=True))
    def test_create_container_exists(self, mock_logger):
        mock_client = FakeClient()
        container_name = "some_container"
        cfa_azure.helpers.create_container(container_name, mock_client)
        mock_logger.debug.assert_called_with(f"Container [{container_name}] already exists. No action needed.")
    
    @patch("cfa_azure.helpers.logger")
    @patch("cfa_azure.helpers.generate_autoscale_formula", MagicMock(return_value=FAKE_AUTOSCALE_FORMULA))
    def test_get_autoscale_formula(self, mock_logger):
        formula = cfa_azure.helpers.get_autoscale_formula()
        mock_logger.debug.assert_called_with("Default autoscale formula used. Please provide a path to autoscale formula to sepcify your own formula.")
        self.assertEqual(formula, FAKE_AUTOSCALE_FORMULA)

    @patch("cfa_azure.helpers.logger")
    def test_get_autoscale_formula_from_text(self, mock_logger):
        text_input = "some formula"
        formula = cfa_azure.helpers.get_autoscale_formula(filepath=None, text_input=text_input)
        mock_logger.debug.assert_called_with("Autoscale formula provided via text input.")
        self.assertEqual(formula, text_input)

    @patch("cfa_azure.helpers.create_container")
    def test_create_blob_containers(self, mock_create_container):
        mock_client = FakeClient()
        expected_calls = [call(FAKE_INPUT_CONTAINER, mock_client), call(FAKE_OUTPUT_CONTAINER, mock_client)]
        cfa_azure.helpers.create_blob_containers(mock_client, FAKE_INPUT_CONTAINER, FAKE_OUTPUT_CONTAINER)
        mock_create_container.assert_has_calls(expected_calls)

    def test_get_batch_pool_json(self):
        batch_json = cfa_azure.helpers.get_batch_pool_json(FAKE_INPUT_CONTAINER, FAKE_OUTPUT_CONTAINER, FAKE_CONFIG, FAKE_AUTOSCALE_FORMULA)
        self.assertEqual(
            batch_json['user_identity']['userAssignedIdentities'], 
            {
                FAKE_CONFIG['Authentication']['user_assigned_identity']: {
                    'clientId': FAKE_CONFIG['Authentication']['client_id'],
                    'principalId': FAKE_CONFIG['Authentication']['principal_id']
                }
            }
        )

    def test_format_extensions(self):
        extension = 'csv'
        formatted = cfa_azure.helpers.format_extensions(extension)
        self.assertEqual(formatted, ['.csv'])

    @patch("os.getenv", MagicMock(return_value="debug"))
    def test_get_log_level(self):
        batch_mgmt_client = FakeClient()
        status = cfa_azure.helpers.check_pool_exists(
            FAKE_RESOURCE_GROUP, 
            FAKE_ACCOUNT, 
            FAKE_BATCH_POOL,
            batch_mgmt_client
        )
        self.assertTrue(status)
