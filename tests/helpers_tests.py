# ruff: noqa: F403, F405

import logging
import unittest
from io import StringIO
from unittest.mock import MagicMock, mock_open, patch

from callee import Contains
from docker.errors import DockerException

import cfa_azure.blob_helpers
import cfa_azure.helpers
from tests.fake_client import *


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
        with self.assertRaises(FileNotFoundError) as exc:
            cfa_azure.helpers.read_config(config_path)
        self.assertEqual(
            f"could not find file {config_path}", str(exc.exception)
        )
        mock_logger.warning.assert_called_with(
            "Configuration file not found. Make sure the location (path) is correct."
        )
        mock_logger.exception.assert_called()

    @patch("cfa_azure.helpers.logger")
    @patch("toml.load", MagicMock(side_effect=Exception))
    def test_read_config_errors(self, mock_logger):
        config_path = "some_path"
        with self.assertRaises(Exception) as exc:
            cfa_azure.helpers.read_config(config_path)
        self.assertEqual(
            "Error occurred while loading the configuration file. Check file format and contents.",
            str(exc.exception),
        )
        mock_logger.warning.assert_called_with(
            "Error occurred while loading the configuration file. Check file format and contents."
        )
        mock_logger.exception.assert_called()

    @patch("cfa_azure.helpers.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    def test_create_container(self, mock_logger):
        mock_client = FakeClient()
        container_name = "some_container"
        cfa_azure.helpers.create_container(container_name, mock_client)
        mock_logger.debug.assert_called_with(
            f"Container [{container_name}] created successfully."
        )

    @patch("cfa_azure.helpers.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    def test_create_container_exists(self, mock_logger):
        mock_client = FakeClient()
        container_name = "some_container"
        cfa_azure.helpers.create_container(container_name, mock_client)
        mock_logger.debug.assert_called_with(
            f"Container [{container_name}] already exists. No action needed."
        )

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
        self.assertEqual(log_level, logging.CRITICAL + 1)

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
        mock_logger.warning.assert_called_with(
            Contains("Did not recognize log level string")
        )

    def test_get_completed_tasks(self):
        batch_client = FakeClient()
        task_summary = cfa_azure.helpers.get_completed_tasks(
            "test_job_id", batch_client=batch_client
        )
        self.assertIsNotNone(task_summary)
        self.assertEqual(task_summary["completed tasks"], 1)

    def test_check_config_req(self):
        status = cfa_azure.helpers.check_config_req(FAKE_CONFIG_MINIMAL)
        self.assertIsNone(status)

    def test_check_config_req_badconfig(self):
        bad_config = {
            "Storage": {
                "storage_account_name": "Test Storage Account",
                "storage_account_url": "Test Storage Account URL",
            }
        }
        status = cfa_azure.helpers.check_config_req(bad_config)
        self.assertIsNotNone(status)

    @patch(
        "cfa_azure.batch_helpers.get_pool_full_info",
        MagicMock(return_value=FAKE_POOL_INFO),
    )
    @patch("cfa_azure.helpers.get_timeout", MagicMock(return_value=10))
    def test_monitor_tasks(self):
        batch_client = FakeClient()
        status = cfa_azure.helpers.monitor_tasks(
            "test_job_id",
            10,
            batch_client,
        )
        self.assertTrue(status["completed"])
        self.assertIsNotNone(status["elapsed time"])

    def test_add_task_to_job(self):
        batch_mgmt_client = FakeClient()
        task_list = cfa_azure.helpers.add_task_to_job(
            "test_job_id",
            "task_id_base",
            docker_command=["some", "docker", "command"],
            batch_client=batch_mgmt_client,
            task_id_max=0,
            task_id_ints=False,
        )
        self.assertIsNotNone(task_list)

    def test_add_task_to_job_input_files_mounts(self):
        batch_mgmt_client = FakeClient()
        task_list = cfa_azure.helpers.add_task_to_job(
            "test_job_id",
            "task_id_base",
            docker_command=["some", "docker", "command"],
            mounts=["some_mount"],
            batch_client=batch_mgmt_client,
            full_container_name=FAKE_INPUT_CONTAINER,
            task_id_max=0,
            task_id_ints=False,
        )
        self.assertIsNotNone(task_list)

    @patch("cfa_azure.helpers.logger")
    @patch("builtins.open", new_callable=mock_open)
    @patch("sys.stdout", new_callable=StringIO)
    @patch(
        "cfa_azure.blob_helpers.download_file", MagicMock(return_value=True)
    )
    def test_download_job_stats(self, mock_logger, mock_file, mock_stdout):
        batch_service_client = FakeClient()
        cfa_azure.helpers.download_job_stats(
            job_id="my_job_id", batch_service_client=batch_service_client
        )
        mock_file.assert_called_with("my_job_id-stats.csv", "a")

    @patch("docker.from_env", MagicMock(return_value=FakeClient()))
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_package_and_upload_dockerfile(self):
        full_container_name = cfa_azure.helpers.package_and_upload_dockerfile(
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name="Fake Repo",
            tag="latest",
            path_to_dockerfile="./Dockerfile",
            use_device_code=False,
        )
        self.assertIsNotNone(full_container_name)

    @patch("docker.from_env", MagicMock(return_value=FakeClient()))
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_package_and_upload_dockerfile_devicecode(self):
        full_container_name = cfa_azure.helpers.package_and_upload_dockerfile(
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name="Fake Repo",
            tag="latest",
            path_to_dockerfile="./Dockerfile",
            use_device_code=True,
        )
        self.assertIsNotNone(full_container_name)

    @patch("cfa_azure.helpers.logger")
    @patch("docker.from_env", MagicMock(return_value=FakeClient()))
    @patch(
        "tests.fake_client.FakeClient.ping",
        MagicMock(side_effect=DockerException),
    )
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_package_and_upload_dockerfile_error(self, mock_logger):
        with self.assertRaises(DockerException) as exc:
            cfa_azure.helpers.package_and_upload_dockerfile(
                registry_name=FAKE_CONTAINER_REGISTRY,
                repo_name="Fake Repo",
                tag="latest",
                path_to_dockerfile="./Dockerfile",
                use_device_code=True,
            )
        self.assertEqual("Make sure Docker is running.", str(exc.exception))
        mock_logger.error.assert_called_with(
            "Could not ping Docker. Make sure Docker is running."
        )
        mock_logger.warning.assert_called_with(
            "Try again when Docker is running."
        )

    def test_check_azure_container_exists(self):
        fake_client = FakeContainerRegistryClient(
            "some_endpoint", "some_credential", "some_audience"
        )
        with patch(
            "cfa_azure.helpers.get_container_registry_client",
            MagicMock(return_value=fake_client),
        ):
            response = cfa_azure.helpers.check_azure_container_exists(
                registry_name=FAKE_CONTAINER_REGISTRY,
                repo_name="Fake Repo",
                tag_name="latest",
                credential=FAKE_CREDENTIAL,
            )
            self.assertTrue(response)

    def test_check_azure_container_exists_missing_tag(self):
        fake_client = FakeContainerRegistryClient(
            "some_endpoint", "some_credential", "some_audience"
        )
        with patch(
            "cfa_azure.helpers.get_container_registry_client",
            MagicMock(return_value=fake_client),
        ):
            response = cfa_azure.helpers.check_azure_container_exists(
                registry_name=FAKE_CONTAINER_REGISTRY,
                repo_name="Fake Repo",
                tag_name="bad_tag_1",
                credential=FAKE_CREDENTIAL,
            )
            self.assertIsNone(response)

    @patch("cfa_azure.helpers.logger")
    def test_add_job(self, mock_logger):
        batch_client = FakeClient()
        job_id = "my_job_id"
        cfa_azure.helpers.add_job(
            job_id, FAKE_BATCH_POOL, batch_client=batch_client
        )
        mock_logger.info.assert_called_with(
            f"Job '{job_id}' created successfully."
        )

    @patch("cfa_azure.helpers.logger")
    def test_add_job_task_failure(self, mock_logger):
        batch_client = FakeClient()
        job_id = "my_job_id"
        cfa_azure.helpers.add_job(
            job_id, FAKE_BATCH_POOL, batch_client=batch_client
        )
        mock_logger.debug.assert_called_with("Attempting to add job.")

    def test_get_timeout(self):
        timeout = cfa_azure.helpers.get_timeout("PT2H10M30S")
        self.assertEqual(timeout, 130)

    def test_get_timeout_hours(self):
        timeout = cfa_azure.helpers.get_timeout("PT2H")
        self.assertEqual(timeout, 120)

    def test_get_timeout_min(self):
        timeout = cfa_azure.helpers.get_timeout("PT10M")
        self.assertEqual(timeout, 10)

    @patch("cfa_azure.helpers.logger")
    @patch("builtins.open", mock_open(read_data="data"))
    @patch("yaml.safe_load", MagicMock(return_value=FAKE_YAML_CONTENT))
    @patch("yaml.dump", MagicMock(return_value=True))
    def test_edit_yaml_r0(self, mock_logger):
        cfa_azure.helpers.edit_yaml_r0(
            file_path="some_yaml_file", r0_start=1, r0_end=4, step=0.1
        )
        mock_logger.debug.assert_called_with("Completed editing YAML files.")

    def test_list_blobs_flat(self):
        blob_service_client = FakeClient()
        blob_names = cfa_azure.blob_helpers.list_blobs_flat(
            container_name=FAKE_INPUT_CONTAINER,
            blob_service_client=blob_service_client,
            verbose=True,
        )
        self.assertEqual(blob_names, FAKE_BLOBS)

    @patch("azure.keyvault.secrets.SecretClient.get_secret")
    @patch(
        "azure.identity.DefaultAzureCredential", MagicMock(return_value=True)
    )
    def test_get_sp_secret(self, mock_secret):
        mock_secret.return_value = FakeClient.FakeSecretClient.FakeSecret()
        secret = cfa_azure.helpers.get_sp_secret(
            config=FAKE_CONFIG, credential=FAKE_CREDENTIAL
        )
        self.assertEqual(secret, FAKE_SECRET)

    @patch(
        "azure.keyvault.secrets.SecretClient.get_secret",
        MagicMock(side_effect=Exception),
    )
    def test_get_sp_secret_bad_key(self):
        with self.assertRaises(Exception):
            cfa_azure.helpers.get_sp_secret(
                config=FAKE_CONFIG, credential=FAKE_CREDENTIAL
            )

    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    def test_list_nodes_by_pool(self):
        compute_nodes = cfa_azure.helpers.list_nodes_by_pool(
            pool_name=FAKE_BATCH_POOL, config=FAKE_CONFIG, node_state="running"
        )
        self.assertEqual(len(compute_nodes), 2)

    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    def test_list_all_nodes_by_pool(self):
        compute_nodes = cfa_azure.helpers.list_nodes_by_pool(
            pool_name=FAKE_BATCH_POOL, config=FAKE_CONFIG
        )
        self.assertEqual(len(compute_nodes), 4)

    @patch("docker.from_env", MagicMock(return_value=FakeClient()))
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_upload_docker_image(self):
        full_container_name = cfa_azure.helpers.upload_docker_image(
            image_name=FAKE_CONTAINER_IMAGE,
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name="Fake Repo",
            tag="latest",
            use_device_code=False,
        )
        self.assertIsNotNone(full_container_name)

    @patch("docker.from_env", MagicMock(side_effect=DockerException))
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_upload_docker_image_exception(self):
        with self.assertRaises(DockerException) as docexc:
            cfa_azure.helpers.upload_docker_image(
                image_name=FAKE_CONTAINER_IMAGE,
                registry_name=FAKE_CONTAINER_REGISTRY,
                repo_name="Fake Repo",
                tag="latest",
                use_device_code=False,
            )
            self.assertEqual(
                "Make sure Docker is running.",
                str(docexc.exception),
            )

    @patch("docker.from_env", MagicMock(return_value=FakeClient()))
    @patch("os.path.exists", MagicMock(return_value=True))
    @patch("subprocess.run", MagicMock(return_value=True))
    def test_upload_docker_image_notag(self):
        full_container_name = cfa_azure.helpers.upload_docker_image(
            image_name=FAKE_CONTAINER_IMAGE,
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name="Fake Repo",
            use_device_code=False,
        )
        self.assertIsNotNone(full_container_name)
