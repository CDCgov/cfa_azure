import unittest
from unittest.mock import patch, MagicMock
from callee import Contains

import cfa_azure
import cfa_azure.batch
from tests.fake_client import *

class TestBatch(unittest.TestCase):

    @patch("builtins.print")
    @patch("cfa_azure.helpers.read_config", MagicMock(return_value=FAKE_CONFIG))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_blob_service_client", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_mgmt_client", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_pool_json", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.create_blob_containers", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.create_batch_pool", MagicMock(return_value=FAKE_BATCH_POOL))
    def test_create_pool(self, mock_print):

        input_container_name = FAKE_INPUT_CONTAINER
        output_container_name = FAKE_OUTPUT_CONTAINER
        config_path = "some_path"
        autoscale_formula_path = "test_formula"
        status = cfa_azure.batch.create_pool(
            FAKE_BATCH_POOL, 
            input_container_name, 
            output_container_name,
            config_path,
            autoscale_formula_path
        )
        mock_print.assert_called_with(Contains('Pool creation process completed'))
        self.assertTrue(status)


    @patch("builtins.print")
    @patch("cfa_azure.helpers.read_config", MagicMock(return_value=FAKE_CONFIG))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_blob_service_client", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_mgmt_client", MagicMock(return_value=FakeClient()))
    @patch("builtins.input", MagicMock(return_value='n'))
    @patch("cfa_azure.helpers.get_batch_pool_json", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.create_blob_containers", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.create_batch_pool", MagicMock(return_value=FAKE_BATCH_POOL))
    def test_create_pool_if_already_exists(self, mock_print):

        input_container_name = FAKE_INPUT_CONTAINER
        output_container_name = FAKE_OUTPUT_CONTAINER
        config_path = "some_path"
        autoscale_formula_path = "test_formula"
        cfa_azure.batch.create_pool(
            FAKE_BATCH_POOL, 
            input_container_name, 
            output_container_name,
            config_path,
            autoscale_formula_path
        )
        mock_print.assert_called_with('No pool created since it already exists. Exiting the process.')

    @patch("builtins.print")
    @patch("toml.load", MagicMock(return_value=FAKE_CONFIG))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_service_client", MagicMock(return_value=FakeClient()))
    @patch("cfa_azure.helpers.add_job", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.add_task_to_job", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.monitor_tasks", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.list_files_in_container", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    def test_run_job(self, mock_print):
        cfa_azure.batch.run_job(
            'test_job_id', 
            'test_task_id', 
            'docker run something', 
            FAKE_INPUT_CONTAINER, 
            FAKE_OUTPUT_CONTAINER
        )
        mock_print.assert_called_with('Job complete. Time to debug. Job not deleted.')

    @patch("builtins.print")
    @patch("toml.load", MagicMock(return_value=FAKE_CONFIG))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_credential", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_batch_service_client", MagicMock(return_value=FakeClient()))
    @patch("cfa_azure.helpers.add_job", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.add_task_to_job", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.monitor_tasks", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.list_files_in_container", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    def test_run_job_no_debugging(self, mock_print):
        cfa_azure.batch.run_job(
            'test_job_id', 
            'test_task_id', 
            'docker run something', 
            FAKE_INPUT_CONTAINER, 
            FAKE_OUTPUT_CONTAINER,
            debug=False
        )
        mock_print.assert_called_with('Cleaning up - deleting job.')


    @patch("builtins.print")
    @patch("subprocess.call", MagicMock(return_value=0))
    def test_package_and_upload_dockerfile(self, mock_print):
        cfa_azure.batch.package_and_upload_dockerfile(FAKE_CONFIG)
        mock_print.assert_called_with('Dockerfile packaged and uploaded successfully.')


    @patch("builtins.print")
    @patch("subprocess.call", MagicMock(return_value=-1))
    def test_package_and_upload_dockerfile_failure(self, mock_print):
        cfa_azure.batch.package_and_upload_dockerfile(FAKE_CONFIG)
        mock_print.assert_called_with('Failed to package and upload Dockerfile.')
