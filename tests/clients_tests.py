# ruff: noqa: F403, F405, F811

import unittest
from unittest.mock import MagicMock, patch

from azure.core.exceptions import HttpResponseError

import cfa_azure.batch_helpers
import cfa_azure.blob_helpers
import cfa_azure.clients
import cfa_azure.helpers
from tests.fake_client import *


class TestClients(unittest.TestCase):
    @patch("cfa_azure.clients.logger")
    @patch(
        "azure.identity.ClientSecretCredential.__init__",
        MagicMock(return_value=None),
    )
    @patch(
        "azure.common.credentials.ServicePrincipalCredentials.__init__",
        MagicMock(return_value=None),
    )
    @patch(
        "cfa_azure.helpers.read_config",
        MagicMock(return_value=FAKE_CONFIG_MINIMAL),
    )
    @patch("cfa_azure.helpers.check_config_req", MagicMock(return_value=True))
    @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    @patch(
        "cfa_azure.blob_helpers.get_blob_service_client",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.batch_helpers.get_batch_mgmt_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    def setUp(self, mock_logger):
        config_path = "some_path"
        self.azure_client = cfa_azure.clients.AzureClient(config_path)
        self.azure_client.pool_name = FAKE_BATCH_POOL
        self.azure_client.pool_parameters = FAKE_POOL_INFO
        mock_logger.info.assert_called_with(
            "Client initialized! Happy coding!"
        )
        self.task_id_ints = False

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.get_deployment_config",
        MagicMock(return_value={"virtualMachineConfiguration": {}}),
    )
    def test_set_pool_info(self, mock_logger):
        self.azure_client.set_pool_info(
            mode="fixed",
            max_autoscale_nodes=3,
            timeout=60,
            dedicated_nodes=1,
            low_priority_nodes=0,
            cache_blobfuse=True,
        )
        mock_logger.debug.assert_called_with("pool parameters generated")

    @patch("cfa_azure.clients.logger")
    def test_set_pool_info_autoscale(self, mock_logger):
        self.azure_client.debug = True
        response = self.azure_client.set_pool_info(
            mode="autoscale",
            max_autoscale_nodes=3,
            timeout=60,
            dedicated_nodes=1,
            low_priority_nodes=0,
            cache_blobfuse=True,
        )
        mock_logger.info.assert_called_with(
            "Either change debugging to False or set the scaling mode to fixed."
        )
        self.assertIsNone(response)

    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=FAKE_CONTAINER_IMAGE),
    )
    @patch(
        "cfa_azure.batch_helpers.get_pool_full_info",
        MagicMock(return_value=FakeClient.FakePool.FakePoolInfo()),
    )
    def test_add_task_nocontainer(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        self.azure_client.task_id_ints = False
        task_list = self.azure_client.add_task(
            "test_job_id",
            docker_cmd=["some", "docker", "command"],
        )
        self.assertIsNotNone(task_list)

    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=FAKE_CONTAINER_IMAGE),
    )
    @patch(
        "cfa_azure.batch_helpers.get_pool_full_info",
        MagicMock(return_value=FakeClient.FakePool.FakePoolInfo()),
    )
    def test_add_task_inputfiles(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        self.azure_client.task_id_ints = False
        task_list = self.azure_client.add_task(
            "test_job_id", docker_cmd=["some", "docker", "command"]
        )
        self.assertIsNotNone(task_list)

    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=FAKE_CONTAINER_IMAGE),
    )
    @patch(
        "cfa_azure.batch_helpers.get_pool_full_info",
        MagicMock(return_value=FakeClient.FakePool.FakePoolInfo()),
    )
    def test_add_task_dependencies(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        self.azure_client.task_id_ints = False
        task_1 = self.azure_client.add_task(
            "test_job_id", docker_cmd=["some", "docker", "command"]
        )
        task_2 = self.azure_client.add_task(
            "test_job_id",
            docker_cmd=["some", "docker", "command"],
            depends_on=[task_1],
        )
        self.assertIsNotNone(task_2)

    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=FAKE_CONTAINER_IMAGE),
    )
    def test_add_task(self):
        self.azure_client.task_id_ints = False
        task_list = self.azure_client.add_task(
            "test_job_id",
            docker_cmd=["some", "docker", "command"],
            container=FAKE_INPUT_CONTAINER,
        )
        self.assertIsNotNone(task_list)

    @patch("cfa_azure.clients.logger")
    def test_set_debugging(self, mock_logger):
        self.azure_client.set_debugging(True)
        self.assertTrue(self.azure_client.debug)
        assert mock_logger.info.call_count == 5

    @patch("cfa_azure.clients.logger")
    def test_set_debugging_badlevel(self, mock_logger):
        self.azure_client.set_debugging("Hello")
        mock_logger.warning.assert_called_with(
            "Please use True or False to set debugging mode."
        )

    @patch("cfa_azure.clients.logger")
    def test_set_debugging_disable(self, mock_logger):
        self.azure_client.set_debugging(False)
        self.assertFalse(self.azure_client.debug)
        mock_logger.debug.assert_called_with("Debugging turned off.")

    @patch(
        "cfa_azure.batch_helpers.get_deployment_config",
        MagicMock(return_value={"virtualMachineConfiguration": {}}),
    )
    def test_create_pool(self):
        self.azure_client.set_pool_info(mode="autoscale")
        pool_details = self.azure_client.create_pool(FAKE_BATCH_POOL)
        self.assertIsNotNone(pool_details)

    @patch("cfa_azure.clients.logger")
    def test_create_pool_no_params(self, mock_logger):
        self.azure_client.pool_parameters = None
        error_msg = "No pool information given. Please use `set_pool_info()` before running `create_pool()`."
        with self.assertRaises(Exception) as exc:
            self.azure_client.create_pool(FAKE_BATCH_POOL)
        mock_logger.exception.assert_called_with(error_msg)
        self.assertEqual(error_msg, str(exc.exception))

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakePool.create",
        MagicMock(
            side_effect=HttpResponseError(message="PropertyCannotBeUpdated")
        ),
    )
    def test_create_pool_response_exists(self, mock_logger):
        self.azure_client.create_pool(FAKE_BATCH_POOL)
        mock_logger.warning.assert_called_with(
            f"Pool {FAKE_BATCH_POOL!r} already exists"
        )

    @patch(
        "tests.fake_client.FakeClient.FakePool.create",
        MagicMock(side_effect=HttpResponseError),
    )
    def test_create_pool_response_error(self):
        with self.assertRaises(HttpResponseError):
            self.azure_client.create_pool(FAKE_BATCH_POOL)

    @patch(
        "cfa_azure.blob_helpers.list_blobs_flat",
        MagicMock(return_value=FAKE_BLOBS),
    )
    def test_list_blob_files(self):
        filenames = self.azure_client.list_blob_files(FAKE_INPUT_CONTAINER)
        self.assertIsNotNone(filenames)

    @patch(
        "cfa_azure.blob_helpers.list_blobs_flat",
        MagicMock(return_value=FAKE_BLOBS),
    )
    def test_list_blob_files_nocontainer(self):
        self.azure_client.mounts = ["some_mounts"]
        filenames = self.azure_client.list_blob_files()
        self.assertIsNotNone(filenames)

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.batch_helpers.get_pool_info",
        MagicMock(return_value=json.dumps(FAKE_POOL_INFO)),
    )
    def test_set_pool(self, mock_logger):
        self.azure_client.mounts = ["some_mounts"]
        self.azure_client.set_pool(FAKE_BATCH_POOL)
        mock_logger.info.assert_called_with(
            "Make sure the VM size matches the use case.\n"
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=False),
    )
    def test_set_pool_nopool(self, mock_logger):
        self.azure_client.set_pool(FAKE_BATCH_POOL)
        mock_logger.info.assert_called_with(
            "Choose an existing pool or create a new pool."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    def test_set_input_container(self, mock_logger):
        self.azure_client.mounts = []
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_input_container(FAKE_INPUT_CONTAINER)
        mock_logger.debug.assert_called_with(
            f"Added input Blob container {FAKE_INPUT_CONTAINER} to AzureClient."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    def test_set_input_container_nocontainer(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_input_container(FAKE_INPUT_CONTAINER)
        mock_logger.warning.assert_called_with(
            f"Container [{FAKE_INPUT_CONTAINER}] does not exist. Please create it if desired."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    def test_set_output_container(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_output_container(FAKE_OUTPUT_CONTAINER)
        mock_logger.debug.assert_called_with(
            f"Added output Blob container {FAKE_OUTPUT_CONTAINER} to AzureClient."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    def test_set_output_container_nocontainer(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_output_container(FAKE_OUTPUT_CONTAINER)
        mock_logger.warning.assert_called_with(
            f"Container [{FAKE_OUTPUT_CONTAINER}] does not exist. Please create it if desired."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    def test_set_blob_container(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_blob_container(
            FAKE_OUTPUT_CONTAINER, rel_mount_dir="relative_mount"
        )
        mock_logger.debug.assert_called_with(
            f"Added Blob container {FAKE_OUTPUT_CONTAINER} to AzureClient."
        )

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    def test_set_blob_container_nocontainer(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.set_blob_container(
            FAKE_OUTPUT_CONTAINER, rel_mount_dir="relative_mount"
        )
        mock_logger.warning.assert_called_with(
            f"Container [{FAKE_OUTPUT_CONTAINER}] does not exist. Please create it if desired."
        )

    @patch(
        "cfa_azure.batch_helpers.get_autoscale_formula",
        MagicMock(return_value=FAKE_AUTOSCALE_FORMULA),
    )
    @patch(
        "cfa_azure.batch_helpers.update_pool",
        MagicMock(
            return_value={
                "pool_id": FAKE_BATCH_POOL,
                "updation_time": "09/01/2024 10:00:00",
            }
        ),
    )
    def test_update_scale_settings_autoscaling(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        pool_info = self.azure_client.update_scale_settings(
            scaling="autoscale",
            autoscale_formula_path="some_path",
            evaluation_interval="PT30M",
        )
        self.assertEqual(pool_info["pool_id"], FAKE_BATCH_POOL)

    @patch(
        "cfa_azure.batch_helpers.get_autoscale_formula",
        MagicMock(return_value=FAKE_AUTOSCALE_FORMULA),
    )
    @patch(
        "cfa_azure.batch_helpers.update_pool",
        MagicMock(
            return_value={
                "pool_id": FAKE_BATCH_POOL,
                "updation_time": "09/01/2024 10:00:00",
            }
        ),
    )
    def test_update_scale_settings_autoscaling_badparams(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        with self.assertRaises(Exception) as exc:
            self.azure_client.update_scale_settings(
                scaling="autoscale",
                dedicated_nodes=10,
                node_deallocation_option="Requeue",
            )
        self.assertEqual(
            "dedicated_nodes, node_deallocation_option cannot be specified with autoscale option",
            str(exc.exception),
        )

    @patch(
        "cfa_azure.batch_helpers.update_pool",
        MagicMock(
            return_value={
                "pool_id": FAKE_BATCH_POOL,
                "updation_time": "09/01/2024 10:00:00",
            }
        ),
    )
    def test_update_scale_settings_fixedscale(self):
        pool_info = self.azure_client.update_scale_settings(
            scaling="fixed",
            pool_name=FAKE_BATCH_POOL,
            dedicated_nodes=10,
            node_deallocation_option="Requeue",
        )
        self.assertEqual(pool_info["pool_id"], FAKE_BATCH_POOL)

    @patch(
        "cfa_azure.batch_helpers.update_pool",
        MagicMock(
            return_value={
                "pool_id": FAKE_BATCH_POOL,
                "updation_time": "09/01/2024 10:00:00",
            }
        ),
    )
    def test_update_scale_settings_fixedscale_badparams(self):
        with self.assertRaises(Exception) as exc:
            self.azure_client.update_scale_settings(
                scaling="fixed",
                pool_name=FAKE_BATCH_POOL,
                autoscale_formula_path="some_path",
                evaluation_interval="PT30M",
            )
        self.assertEqual(
            "autoscale_formula_path, evaluation_interval cannot be specified with fixed option",
            str(exc.exception),
        )

    @patch(
        "cfa_azure.batch_helpers.update_pool",
        MagicMock(
            return_value={
                "pool_id": FAKE_BATCH_POOL,
                "updation_time": "09/01/2024 10:00:00",
            }
        ),
    )
    def test_update_scale_settings_fixedscale_spot(self):
        pool_info = self.azure_client.update_scale_settings(
            scaling="fixed", pool_name=FAKE_BATCH_POOL, low_priority_nodes=10
        )
        self.assertEqual(pool_info["pool_id"], FAKE_BATCH_POOL)

    @patch("cfa_azure.helpers.add_job", MagicMock(return_value=True))
    def test_add_job(self):
        self.azure_client.add_job(
            job_id="fake_job_id", pool_name=FAKE_BATCH_POOL
        )
        self.assertEqual(len(self.azure_client.jobs), 1)

    @patch("cfa_azure.helpers.add_job", MagicMock(return_value=True))
    def test_add_job_default(self):
        self.azure_client.pool_name = FAKE_BATCH_POOL
        self.azure_client.add_job(job_id="fake_job_id")
        self.assertEqual(len(self.azure_client.jobs), 1)

    @patch("cfa_azure.clients.logger")
    @patch("cfa_azure.helpers.check_job_exists", MagicMock(return_value=True))
    @patch(
        "cfa_azure.helpers.get_completed_tasks",
        MagicMock(return_value=[FakeClient.FakeTask()]),
    )
    @patch(
        "cfa_azure.helpers.check_job_complete", MagicMock(return_value=True)
    )
    def test_check_job_status(self, mock_logger):
        job_id = "my_job_id"
        self.azure_client.check_job_status(job_id)
        mock_logger.info.assert_called_with(f"Job {job_id} completed.")

    @patch("cfa_azure.clients.logger")
    @patch("cfa_azure.helpers.check_job_exists", MagicMock(return_value=True))
    @patch(
        "cfa_azure.helpers.get_completed_tasks",
        MagicMock(return_value=[FakeClient.FakeTask()]),
    )
    @patch(
        "cfa_azure.helpers.check_job_complete", MagicMock(return_value=False)
    )
    @patch(
        "cfa_azure.helpers.get_job_state", MagicMock(return_value="running")
    )
    def test_check_job_status_running(self, mock_logger):
        job_id = "my_job_id"
        self.azure_client.check_job_status(job_id)
        mock_logger.info.assert_called_with("Job in running state")

    @patch("cfa_azure.clients.logger")
    @patch("cfa_azure.helpers.check_job_exists", MagicMock(return_value=False))
    def test_check_job_status_noexist(self, mock_logger):
        job_id = "my_job_id"
        self.azure_client.check_job_status(job_id)
        mock_logger.info.assert_called_with(f"Job {job_id} does not exist.")

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.delete_pool",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.create_batch_pool",
        MagicMock(return_value=FAKE_BATCH_POOL),
    )
    def test_update_container_set(self, mock_logger):
        containers = [
            {"name": FAKE_INPUT_CONTAINER, "relative_mount_dir": "input"},
            {"name": FAKE_OUTPUT_CONTAINER, "relative_mount_dir": "output"},
            {"name": "Fake Logging Container", "relative_mount_dir": "logs"},
        ]
        pool_name = self.azure_client.update_container_set(
            containers=containers, force_update=False
        )
        mock_logger.error.assert_called_with(
            f"There are 2 compute nodes actively running tasks in pool {FAKE_BATCH_POOL}. Please wait for jobs to complete or retry withy force_update=True."
        )
        self.assertIsNone(pool_name)

    # @patch(
    #    "cfa_azure.batch_helpers.check_pool_exists",
    #    MagicMock(return_value=True),
    # )
    # @patch(
    #    "cfa_azure.helpers.get_batch_service_client",
    #    MagicMock(return_value=FakeClient()),
    # )
    # @patch(
    #    "cfa_azure.batch_helpers.delete_pool",
    #    MagicMock(return_value=FakeClient()),
    # )
    # @patch(
    #    "cfa_azure.helpers.format_rel_path",
    #    MagicMock(return_value="/some_path"),
    # )
    # @patch(
    #    "cfa_azure.batch_helpers.create_batch_pool",
    #    MagicMock(return_value=FAKE_BATCH_POOL),
    # )
    # @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    # def test_update_container_set_forced(self):
    #    self.azure_client.blob_service_client = FakeClient()
    #    containers = [
    #        {"name": FAKE_INPUT_CONTAINER, "relative_mount_dir": "/input"},
    #        {"name": FAKE_OUTPUT_CONTAINER, "relative_mount_dir": "/output"},
    #        {"name": "Fake Logging Container", "relative_mount_dir": "/logs"},
    #    ]
    #    pool_name = self.azure_client.update_container_set(
    #        pool_name=FAKE_BATCH_POOL, containers=containers, force_update=True
    #    )
    #    self.assertEqual(pool_name, FAKE_BATCH_POOL)

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.helpers.format_rel_path",
        MagicMock(return_value="/some_path"),
    )
    @patch("cfa_azure.helpers.create_container", MagicMock(return_value=True))
    def test_create_input_container(self, mock_logger):
        self.azure_client.create_input_container(name=FAKE_INPUT_CONTAINER)
        self.assertEqual(
            FAKE_INPUT_CONTAINER, self.azure_client.input_container_name
        )
        mock_logger.debug.assert_called_with(
            "Created container client for input container."
        )

    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=False),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.delete_pool",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.create_batch_pool",
        MagicMock(return_value=FAKE_BATCH_POOL),
    )
    def test_update_containers_new_pool(self):
        containers = [
            {"name": FAKE_INPUT_CONTAINER, "relative_mount_dir": "input"},
            {"name": FAKE_OUTPUT_CONTAINER, "relative_mount_dir": "output"},
            {"name": "Fake Logging Container", "relative_mount_dir": "logs"},
        ]
        pool_name = self.azure_client.update_container_set(
            pool_name=FAKE_BATCH_POOL,
            containers=containers,
            force_update=False,
        )
        self.assertEqual(pool_name, FAKE_BATCH_POOL)

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.delete_pool",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.create_batch_pool",
        MagicMock(return_value=FAKE_BATCH_POOL),
    )
    def test_update_containers(self, mock_logger):
        pool_name = self.azure_client.update_containers(
            input_container_name=FAKE_INPUT_CONTAINER,
            output_container_name=FAKE_OUTPUT_CONTAINER,
            force_update=False,
        )
        mock_logger.error.assert_called_with(
            f"There are 2 compute nodes actively running tasks in pool {FAKE_BATCH_POOL}. Please wait for jobs to complete or retry withy force_update=True."
        )
        self.assertIsNone(pool_name)

    # @patch("cfa_azure.helpers.get_sp_secret", MagicMock(return_value=True))
    # @patch(
    #    "cfa_azure.helpers.get_batch_service_client",
    #    MagicMock(return_value=FakeClient()),
    # )
    # @patch(
    #    "cfa_azure.batch_helpers.delete_pool",
    #    MagicMock(return_value=FakeClient()),
    # )
    # @patch(
    #    "cfa_azure.batch_helpers.create_batch_pool",
    #    MagicMock(return_value=FAKE_BATCH_POOL),
    # )
    # @patch(
    #    "cfa_azure.batch_helpers.check_pool_exists",
    #    MagicMock(return_value=True),
    # )
    # def test_update_containers_forced(self):
    #    pool_name = self.azure_client.update_containers(
    #        pool_name=FAKE_BATCH_POOL,
    #        input_container_name=FAKE_INPUT_CONTAINER,
    #        output_container_name=FAKE_OUTPUT_CONTAINER,
    #        force_update=True,
    #    )
    #    self.assertEqual(pool_name, FAKE_BATCH_POOL)

    @patch(
        "cfa_azure.batch_helpers.check_pool_exists",
        MagicMock(return_value=False),
    )
    @patch(
        "cfa_azure.helpers.get_batch_service_client",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.delete_pool",
        MagicMock(return_value=FakeClient()),
    )
    @patch(
        "cfa_azure.batch_helpers.create_batch_pool",
        MagicMock(return_value=FAKE_BATCH_POOL),
    )
    def test_update_containers_new_pool(self):
        pool_name = self.azure_client.update_containers(
            pool_name=FAKE_BATCH_POOL,
            input_container_name=FAKE_INPUT_CONTAINER,
            output_container_name=FAKE_OUTPUT_CONTAINER,
            force_update=False,
        )
        self.assertEqual(pool_name, FAKE_BATCH_POOL)

    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=FAKE_CONTAINER_IMAGE),
    )
    def test_set_azure_container(self):
        container_name = self.azure_client.set_azure_container(
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name=FAKE_REPO_NAME,
            tag_name="latest",
        )
        self.assertEqual(container_name, FAKE_CONTAINER_IMAGE)

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.helpers.check_azure_container_exists",
        MagicMock(return_value=None),
    )
    def test_set_azure_container_no_name(self, mock_logger):
        container_name = self.azure_client.set_azure_container(
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name=FAKE_REPO_NAME,
            tag_name="latest",
        )
        mock_logger.warning.assert_called_with("ACR container does not exist.")
        self.assertIsNone(container_name)

    @patch(
        "cfa_azure.batch_helpers.get_deployment_config",
        MagicMock(return_value={"virtualMachineConfiguration": {}}),
    )
    @patch(
        "cfa_azure.helpers.package_and_upload_dockerfile",
        MagicMock(return_value=FAKE_INPUT_CONTAINER),
    )
    def test_package_and_upload_dockerfile(self):
        self.azure_client.set_pool_info(mode="autoscale")
        container_name = self.azure_client.package_and_upload_dockerfile(
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name=FAKE_REPO_NAME,
            tag="latest",
        )
        self.assertEqual(FAKE_INPUT_CONTAINER, container_name)

    @patch("cfa_azure.clients.logger")
    @patch(
        "cfa_azure.batch_helpers.get_deployment_config",
        MagicMock(return_value={"virtualMachineConfiguration": {}}),
    )
    @patch(
        "cfa_azure.helpers.upload_docker_image",
        MagicMock(return_value=FAKE_INPUT_CONTAINER),
    )
    def test_upload_docker_image(self, mock_logger):
        self.azure_client.set_pool_info(mode="autoscale")
        container_name = self.azure_client.upload_docker_image(
            image_name=FAKE_CONTAINER_IMAGE,
            registry_name=FAKE_CONTAINER_REGISTRY,
            repo_name=FAKE_REPO_NAME,
            tag="latest",
        )
        mock_logger.debug.assert_called_with(
            "Completed package_and_upload_docker_image() function."
        )
        self.assertEqual(FAKE_INPUT_CONTAINER, container_name)

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    def test_upload_files_no_container(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        error_message = f"Blob container {FAKE_INPUT_CONTAINER} does not exist. Please try again with an existing Blob container."
        with self.assertRaises(Exception) as exc:
            self.azure_client.upload_files(
                files=FAKE_FOLDER_CONTENTS, container_name=FAKE_INPUT_CONTAINER
            )
        self.assertEqual(error_message, str(exc.exception))
        mock_logger.error.assert_called_with(error_message)

    @patch("cfa_azure.clients.logger")
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
    def test_upload_files(self, mock_logger):
        self.azure_client.blob_service_client = FakeClient()
        self.azure_client.upload_files(
            files=FAKE_FOLDER_CONTENTS, container_name=FAKE_INPUT_CONTAINER
        )
        mock_logger.debug.assert_called_with(
            "Uploaded all files in files list."
        )

    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    def test_read_blob(self):
        self.azure_client.blob_service_client = FakeClient()
        data = self.azure_client.read_blob(
            blob_url="somefolder/somefile.csv", container="some_container"
        )
        self.assertIsNotNone(data)
