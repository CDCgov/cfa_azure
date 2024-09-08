import unittest
from unittest.mock import patch, MagicMock, call
import cfa_azure.helpers
from tests.fake_client import *
from callee import Contains
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
    @patch("toml.load", MagicMock(side_effect=FileNotFoundError))
    def test_read_config_nofile(self, mock_logger):
        config_path = "some_path"
        cfa_azure.helpers.read_config(config_path)
        mock_logger.warning.assert_called_with("Configuration file not found. Make sure the location (path) is correct.")
        mock_logger.exception.assert_called()

    @patch("cfa_azure.helpers.logger")
    @patch("toml.load", MagicMock(side_effect=Exception ))
    def test_read_config_errors(self, mock_logger):
        config_path = "some_path"
        cfa_azure.helpers.read_config(config_path)
        mock_logger.warning.assert_called_with("Error occurred while loading the configuration file. Check file format and contents.")
        mock_logger.exception.assert_called()

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

    def test_check_pool_exists(self):
        batch_mgmt_client = FakeClient()
        status = cfa_azure.helpers.check_pool_exists(
            FAKE_RESOURCE_GROUP, 
            FAKE_ACCOUNT, 
            FAKE_BATCH_POOL,
            batch_mgmt_client
        )
        self.assertTrue(status)
    
    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="debug"))
    @patch("tests.fake_client.FakeClient.FakePool.get", MagicMock(side_effect=Exception))
    def test_check_pool_exists_error(self, mock_logger):
        batch_mgmt_client = FakeClient()
        status = cfa_azure.helpers.check_pool_exists(
            FAKE_RESOURCE_GROUP, 
            FAKE_ACCOUNT, 
            FAKE_BATCH_POOL,
            batch_mgmt_client
        )
        self.assertFalse(status)
        mock_logger.debug.assert_called_with("Pool does not exist.")

    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="debug"))
    def test_get_log_level_debug(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.DEBUG)
        mock_logger.info.assert_called_with("Log level set to DEBUG")
 
    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="info"))
    def test_get_log_level_info(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.INFO)
        mock_logger.info.assert_called_with("Log level set to INFO")
        
    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="warn"))
    def test_get_log_level_warning(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.WARNING)
        mock_logger.info.assert_called_with("Log level set to WARNING")

    @patch("os.getenv", MagicMock(return_value="none"))
    def test_get_log_level_not_specified(self):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.CRITICAL+1)

    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="error"))
    def test_get_log_level_error(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.ERROR)
        mock_logger.info.assert_called_with("Log level set to ERROR")

    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="critical"))
    def test_get_log_level_critical(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.CRITICAL)
        mock_logger.info.assert_called_with("Log level set to CRITICAL")
    
    @patch("cfa_azure.helpers.logger")
    @patch("os.getenv", MagicMock(return_value="11"))
    def test_get_log_level_invalid(self, mock_logger):
        log_level = cfa_azure.helpers.get_log_level()
        self.assertEqual(log_level, logging.DEBUG)
        mock_logger.warning.assert_called_with(Contains("Did not recognize log level string"))
    
    def fake_format_extensions(*args):
        return [args[0]]

    @patch("cfa_azure.helpers.format_extensions", MagicMock(side_effect=fake_format_extensions))
    @patch("tests.fake_client.FakeClient.FakeContainerClient.exists", MagicMock(return_value=True))
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch("cfa_azure.helpers.walk_folder", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    @patch("cfa_azure.helpers.upload_blob_file", MagicMock(return_value=True))
    def test_upload_files_in_folder(self):
        blob_service_client = FakeClient()
        uploaded_files = cfa_azure.helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            include_extensions='.csv',
            exclude_extensions='.txt', 
            location_in_blob="", 
            blob_service_client=blob_service_client, 
            verbose=True, 
            force_upload=True
        )
        self.assertEqual(uploaded_files, FAKE_FOLDER_CONTENTS)

    @patch("cfa_azure.helpers.format_extensions", MagicMock(side_effect=fake_format_extensions))
    @patch("tests.fake_client.FakeClient.FakeContainerClient.exists", MagicMock(return_value=True))
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch("cfa_azure.helpers.walk_folder", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    @patch("builtins.input", MagicMock(return_value='y'))
    @patch("cfa_azure.helpers.upload_blob_file", MagicMock(return_value=True))
    def test_upload_files_in_folder_exclusions(self):
        blob_service_client = FakeClient()
        uploaded_files = cfa_azure.helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions='.txt', 
            location_in_blob="", 
            blob_service_client=blob_service_client, 
            verbose=True, 
            force_upload=True
        )
        self.assertEqual(uploaded_files, FAKE_FOLDER_CONTENTS)

    @patch("cfa_azure.helpers.format_extensions", MagicMock(side_effect=fake_format_extensions))
    @patch("tests.fake_client.FakeClient.FakeContainerClient.exists", MagicMock(return_value=True))
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch("cfa_azure.helpers.walk_folder", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    @patch("builtins.input", MagicMock(return_value='y'))
    @patch("cfa_azure.helpers.upload_blob_file", MagicMock(return_value=True))
    def test_upload_files_in_folder_exclusions_forced(self):
        blob_service_client = FakeClient()
        uploaded_files = cfa_azure.helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions='.txt', 
            location_in_blob="", 
            blob_service_client=blob_service_client, 
            verbose=True, 
            force_upload=False
        )
        self.assertEqual(uploaded_files, FAKE_FOLDER_CONTENTS)

    @patch("cfa_azure.helpers.get_pool_full_info", MagicMock(return_value=FAKE_POOL_INFO))
    @patch("cfa_azure.helpers.get_timeout", MagicMock(return_value=10))
    def test_monitor_tasks(self):
        batch_client = FakeClient()
        batch_mgmt_client = FakeClient()
        status = cfa_azure.helpers.monitor_tasks(
            "test_job_id",
            10,
            batch_client,
            FAKE_RESOURCE_GROUP,
            FAKE_ACCOUNT,
            FAKE_BATCH_POOL,
            batch_mgmt_client
        )
        self.assertTrue(status['completed'])
        self.assertIsNotNone(status['elapsed time'])

    def test_add_task_to_job(self):
        batch_mgmt_client = FakeClient()
        task_list = cfa_azure.helpers.add_task_to_job(
            "test_job_id", 
            "task_id_base",
            docker_command=["some", "docker", "command"], 
            input_files=None, 
            mounts=None, 
            depends_on=None, 
            batch_client=batch_mgmt_client, 
            full_container_name=None, 
            task_id_max=0
        )
        self.assertIsNotNone(task_list)

    def test_add_task_to_job_input_files_mounts(self):
            batch_mgmt_client = FakeClient()
            task_list = cfa_azure.helpers.add_task_to_job(
                "test_job_id", 
                "task_id_base",
                docker_command=["some", "docker", "command"], 
                input_files=["test_file_1.sh"], 
                mounts=["some_mount"], 
                depends_on=None, 
                batch_client=batch_mgmt_client, 
                full_container_name=FAKE_INPUT_CONTAINER, 
                task_id_max=0
            )
            self.assertIsNotNone(task_list)