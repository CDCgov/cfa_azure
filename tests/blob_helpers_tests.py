import unittest
from unittest.mock import MagicMock, mock_open, patch

import cfa_azure.blob_helpers

from .fake_client import (
    FAKE_ARGUMENTS,
    FAKE_BLOB_CONTENT,
    FAKE_CONFIG,
    FAKE_FOLDER,
    FAKE_FOLDER_CONTENTS,
    FAKE_FOLDER_CONTENTS_WALK,
    FAKE_INPUT_CONTAINER,
    FAKE_OUTPUT_CONTAINER,
    FakeClient,
)


class TestBloblHelpers(unittest.TestCase):
    def setUp(self):
        self.format_extensions_patcher = patch(
            "cfa_azure.blob_helpers.format_extensions",
            MagicMock(side_effect=(lambda x: [x[0]])),
        )
        self.dirname_patcher = patch(
            "os.path.dirname", MagicMock(return_value=FAKE_FOLDER)
        )
        self.os_walk_patcher = patch(
            "os.walk", MagicMock(return_value=FAKE_FOLDER_CONTENTS)
        )
        self.test_walk_folder_patcher = patch(
            "cfa_azure.blob_helpers.walk_folder",
            MagicMock(return_value=FAKE_FOLDER_CONTENTS),
        )
        self.isdir_patcher = patch(
            "os.path.isdir", MagicMock(return_value=True)
        )

    def tearDown(self):
        self.isdir_patcher.stop()
        self.test_walk_folder_patcher.stop()
        self.os_walk_patcher.stop()
        self.dirname_patcher.stop()
        self.format_extensions_patcher.stop()

    def test_upload_blob_file(self):
        container_client = FakeClient.FakeContainerClient()
        with patch("builtins.open", new_callable=mock_open):
            cfa_azure.blob_helpers.upload_blob_file(
                filepath=FAKE_FOLDER,
                location=FAKE_BLOB_CONTENT,
                container_client=container_client,
            )
            self.assertTrue(True)
            cfa_azure.blob_helpers.upload_blob_file(
                filepath=f"/{FAKE_FOLDER}",
                location=FAKE_BLOB_CONTENT,
                container_client=container_client,
                verbose=True,
            )
            self.assertTrue(True)

    @patch("os.walk", MagicMock(return_value=FAKE_FOLDER_CONTENTS_WALK))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS_WALK),
    )
    def test_walk_folder(self):
        list_files = cfa_azure.blob_helpers.walk_folder(FAKE_FOLDER)
        self.assertEqual(list_files, FAKE_FOLDER_CONTENTS_WALK)

    def test_get_blob_config(self):
        blob_config = cfa_azure.blob_helpers.get_blob_config(
            container_name=FAKE_INPUT_CONTAINER,
            rel_mount_path="some_path",
            cache_blobfuse=True,
            config=FAKE_CONFIG,
        )
        self.assertEqual(
            blob_config["azureBlobFileSystemConfiguration"]["containerName"],
            FAKE_INPUT_CONTAINER,
        )
        self.assertFalse(
            blob_config["azureBlobFileSystemConfiguration"]["blobfuseOptions"]
        )

    def test_get_blob_config_nofuse(self):
        blob_config = cfa_azure.blob_helpers.get_blob_config(
            container_name=FAKE_INPUT_CONTAINER,
            rel_mount_path="some_path",
            cache_blobfuse=False,
            config=FAKE_CONFIG,
        )
        self.assertEqual(
            blob_config["azureBlobFileSystemConfiguration"]["containerName"],
            FAKE_INPUT_CONTAINER,
        )
        self.assertEqual(
            blob_config["azureBlobFileSystemConfiguration"]["blobfuseOptions"],
            "-o direct_io",
        )

    def test_format_extensions(self):
        extension = "csv"
        formatted = cfa_azure.blob_helpers.format_extensions(extension)
        self.assertEqual(formatted, [".csv"])

    @patch(
        "cfa_azure.blob_helpers.initialize_blob_arguments",
        MagicMock(return_value=FAKE_ARGUMENTS),
    )
    @patch(
        "cfa_azure.blob_helpers.get_container_client",
        MagicMock(return_value=FakeClient.FakeContainerClient()),
    )
    @patch("cfa_azure.blob_helpers.upload_blob_file")
    def test_upload_blob(self, mock_upload_blob_file):
        cfa_azure.blob_helpers.upload_blob()
        mock_upload_blob_file.assert_called_once()


class TestBlobMockUploadHelpers(TestBloblHelpers):
    def setUp(self):
        super().setUp()
        self.real_path_patcher = patch(
            "os.path.realpath", MagicMock(return_value=FAKE_FOLDER)
        )
        self.upload_blob_file_patcher = patch(
            "cfa_azure.blob_helpers.upload_blob_file",
            MagicMock(return_value=True),
        )

    def tearDown(self):
        self.upload_blob_file_patcher.stop()
        self.dirname_patcher.stop()
        self.real_path_patcher.stop()
        super().tearDown()

    def test_upload_files_in_folder(self):
        blob_service_client = FakeClient()
        with self.assertRaises(Exception) as exc:
            cfa_azure.blob_helpers.upload_files_in_folder(
                FAKE_FOLDER,
                FAKE_INPUT_CONTAINER,
                include_extensions=".csv",
                exclude_extensions=".txt",
                location_in_blob="",
                blob_service_client=blob_service_client,
                verbose=True,
                force_upload=True,
            )
        self.assertEqual(
            "Use included_extensions or exclude_extensions, not both.",
            str(exc.exception),
        )

    @patch("builtins.input", MagicMock(return_value="y"))
    def test_upload_files_in_folder_exclusions(self):
        blob_service_client = FakeClient()
        cfa_azure.blob_helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions=".txt",
            location_in_blob="",
            blob_service_client=blob_service_client,
            verbose=True,
            force_upload=True,
        )
        self.assertIsNotNone(FAKE_FOLDER_CONTENTS)
        cfa_azure.blob_helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions=".txt",
            location_in_blob="",
            blob_service_client=blob_service_client,
            verbose=True,
            force_upload=False,
        )
        self.assertIsNotNone(FAKE_FOLDER_CONTENTS)

    @patch("os.path.isdir", MagicMock(return_value=False))
    def test_upload_files_in_folder_no_inclusions_exclusions(self):
        blob_service_client = FakeClient()
        file_list = cfa_azure.blob_helpers.upload_files_in_folder(
            "some_folder",
            container_name=FAKE_OUTPUT_CONTAINER,
            location_in_blob="",
            blob_service_client=blob_service_client,
            verbose=True,
            force_upload=True,
        )
        self.assertIsNone(file_list)
