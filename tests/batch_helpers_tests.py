import unittest
from unittest.mock import MagicMock, patch

import cfa_azure.batch_helpers

from .fake_client import (
    FAKE_ACCOUNT,
    FAKE_AUTOSCALE_FORMULA,
    FAKE_BATCH_POOL,
    FAKE_CONFIG,
    FAKE_CONFIG_MINIMAL,
    FAKE_CONTAINER_IMAGE,
    FAKE_CONTAINER_REGISTRY,
    FAKE_CREDENTIAL,
    FAKE_INPUT_CONTAINER,
    FAKE_OUTPUT_CONTAINER,
    FAKE_RESOURCE_GROUP,
    FakeClient,
)


class TestBatchHelpers(unittest.TestCase):
    def test_get_batch_pool_json_no_autoscale(self):
        batch_json = cfa_azure.batch_helpers.get_batch_pool_json(
            FAKE_INPUT_CONTAINER, FAKE_OUTPUT_CONTAINER, FAKE_CONFIG
        )
        self.assertFalse(
            "autoScale" in batch_json["pool_parameters"]["properties"]
        )

    def test_create_batch_pool(self):
        batch_mgmt_client = FakeClient()
        with patch("cfa_azure.batch_helpers.logger") as mock_logger:
            batch_json = {
                "pool_id": FAKE_BATCH_POOL,
                "resource_group_name": FAKE_RESOURCE_GROUP,
                "account_name": FAKE_ACCOUNT,
                "pool_parameters": "some parameters",
            }
            pool_id = cfa_azure.batch_helpers.create_batch_pool(
                batch_mgmt_client, batch_json
            )
            mock_logger.info.assert_called_with(
                f"Pool {pool_id!r} created successfully."
            )
            self.assertEqual(pool_id, FAKE_BATCH_POOL)

    def test_get_autoscale_formula_from_text(self):
        text_input = "some formula"
        with patch("cfa_azure.batch_helpers.logger") as mock_logger:
            formula = cfa_azure.batch_helpers.get_autoscale_formula(
                text_input=text_input
            )
            mock_logger.debug.assert_called_with(
                "Autoscale formula provided via text input."
            )
            self.assertEqual(formula, text_input)

    def test_get_pool_parameters_bad_mode(self):
        response = cfa_azure.batch_helpers.get_pool_parameters(
            mode="bad_mode",
            container_image_name=FAKE_CONTAINER_IMAGE,
            container_registry_url=FAKE_CONTAINER_REGISTRY,
            container_registry_server=FAKE_CONTAINER_REGISTRY,
            config=FAKE_CONFIG_MINIMAL,
            mount_config=[],
            credential=FAKE_CREDENTIAL,
            autoscale_formula_path="some_autoscale_formula",
            timeout=60,
            dedicated_nodes=1,
            low_priority_nodes=0,
            use_default_autoscale_formula=False,
            max_autoscale_nodes=3,
        )
        self.assertEqual(response, {})

    def test_check_pool_exists(self):
        batch_mgmt_client = FakeClient()
        status = cfa_azure.batch_helpers.check_pool_exists(
            FAKE_RESOURCE_GROUP,
            FAKE_ACCOUNT,
            FAKE_BATCH_POOL,
            batch_mgmt_client,
        )
        self.assertTrue(status)

    @patch("os.getenv", MagicMock(return_value="debug"))
    @patch(
        "tests.fake_client.FakeClient.FakePool.get",
        MagicMock(side_effect=Exception),
    )
    def test_check_pool_exists_error(self):
        batch_mgmt_client = FakeClient()
        with patch("cfa_azure.batch_helpers.logger") as mock_logger:
            status = cfa_azure.batch_helpers.check_pool_exists(
                FAKE_RESOURCE_GROUP,
                FAKE_ACCOUNT,
                FAKE_BATCH_POOL,
                batch_mgmt_client,
            )
            self.assertFalse(status)
            mock_logger.debug.assert_called_with("Pool does not exist.")

    class TestBatchPoolHelpers(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.autoscale_patcher = patch(
                "cfa_azure.batch_helpers.get_autoscale_formula",
                MagicMock(return_value=FAKE_AUTOSCALE_FORMULA),
            )

        @classmethod
        def tearDownClass(cls):
            cls.autoscale_patcher.stop()

        def test_get_batch_pool_json(self):
            batch_json = cfa_azure.batch_helpers.get_batch_pool_json(
                FAKE_INPUT_CONTAINER,
                FAKE_OUTPUT_CONTAINER,
                FAKE_CONFIG,
                FAKE_AUTOSCALE_FORMULA,
            )
            self.assertEqual(
                batch_json["user_identity"]["userAssignedIdentities"],
                {
                    FAKE_CONFIG["Authentication"]["user_assigned_identity"]: {
                        "clientId": FAKE_CONFIG["Authentication"][
                            "batch_application_id"
                        ],
                        "principalId": FAKE_CONFIG["Authentication"][
                            "batch_object_id"
                        ],
                    }
                },
            )

        def test_get_batch_pool_json_custominterval(self):
            batch_json = cfa_azure.batch_helpers.get_batch_pool_json(
                input_container_name=FAKE_INPUT_CONTAINER,
                output_container_name=FAKE_OUTPUT_CONTAINER,
                config=FAKE_CONFIG,
                autoscale_formula_path=FAKE_AUTOSCALE_FORMULA,
                autoscale_evaluation_interval="PT35M",
                fixedscale_resize_timeout="PT45M",
            )
            self.assertEqual(
                batch_json["pool_parameters"]["properties"]["scaleSettings"][
                    "autoScale"
                ]["evaluationInterval"],
                "PT35M",
            )
            self.assertEqual(
                batch_json["pool_parameters"]["properties"][
                    "resizeOperationStatus"
                ]["resizeTimeout"],
                "PT45M",
            )

        def test_get_autoscale_formula(self):
            with patch("cfa_azure.batch_helpers.logger") as mock_logger:
                formula = cfa_azure.batch_helpers.get_autoscale_formula()
                mock_logger.debug.assert_called_with(
                    "Default autoscale formula used. Please provide a path to autoscale formula to sepcify your own formula."
                )
                self.assertEqual(formula, FAKE_AUTOSCALE_FORMULA)

    class TestBatchPoolMockVMHelpers(TestBatchPoolHelpers):
        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            cls.deploy_patcher = patch(
                "cfa_azure.batch_helpers.get_deployment_config",
                MagicMock(return_value={"virtualMachineConfiguration": {}}),
            )

        @classmethod
        def tearDownClass(cls):
            cls.deploy_patcher.stop()
            super().tearDownClass()

        def test_get_pool_parameters(self):
            response = cfa_azure.batch_helpers.get_pool_parameters(
                mode="autoscale",
                container_image_name=FAKE_CONTAINER_IMAGE,
                container_registry_url=FAKE_CONTAINER_REGISTRY,
                container_registry_server=FAKE_CONTAINER_REGISTRY,
                config=FAKE_CONFIG_MINIMAL,
                mount_config=[],
                credential=FAKE_CREDENTIAL,
                autoscale_formula_path="some_autoscale_formula",
                timeout=60,
                dedicated_nodes=1,
                low_priority_nodes=0,
                use_default_autoscale_formula=False,
                max_autoscale_nodes=3,
            )
            self.assertIsNotNone(response)

        def test_get_pool_parameters_use_default(self):
            response = cfa_azure.batch_helpers.get_pool_parameters(
                mode="autoscale",
                container_image_name=FAKE_CONTAINER_IMAGE,
                container_registry_url=FAKE_CONTAINER_REGISTRY,
                container_registry_server=FAKE_CONTAINER_REGISTRY,
                config=FAKE_CONFIG_MINIMAL,
                mount_config=[],
                credential=FAKE_CREDENTIAL,
                autoscale_formula_path="some_autoscale_formula",
                timeout=60,
                dedicated_nodes=1,
                low_priority_nodes=0,
                use_default_autoscale_formula=True,
                max_autoscale_nodes=3,
            )
            self.assertIsNotNone(response)
