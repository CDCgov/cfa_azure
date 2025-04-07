import unittest
from unittest.mock import MagicMock, call, patch

import cfa_azure.batch_helpers
import cfa_azure.blob_helpers
import cfa_azure.helpers
from tests.fake_client import *


class TestBloblHelpers(unittest.TestCase):
    # @patch("cfa_azure.helpers.create_container")
    # def test_create_blob_containers(self, mock_create_container):
    #    mock_client = FakeClient()
    #    expected_calls = [
    #        call(FAKE_INPUT_CONTAINER, mock_client),
    #        call(FAKE_OUTPUT_CONTAINER, mock_client),
    #    ]
    #    cfa_azure.blob_helpers.create_blob_containers(
    #        mock_client, FAKE_INPUT_CONTAINER, FAKE_OUTPUT_CONTAINER
    #    )
    #    mock_create_container.assert_has_calls(expected_calls)

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

    # @patch("cfa_azure.helpers.logger")
    # @patch(
    #    "cfa_azure.blob_helpers.check_virtual_directory_existence",
    #    MagicMock(return_value=True),
    # )
    # @patch(
    #    "cfa_azure.blob_helpers.download_file", MagicMock(return_value=True)
    # )
    # def test_download_directory(self, mock_logger):
    #    blob_service_client = FakeClient()
    #    cfa_azure.blob_helpers.download_directory(
    #        container_name=FAKE_INPUT_CONTAINER,
    #        src_path="some_path/",
    #        dest_path="another_path",
    #        blob_service_client=blob_service_client,
    #        include_extensions=".csv",
    #        verbose=True,
    #    )
    #    mock_logger.debug.assert_called_with("Download complete.")

    # @patch("cfa_azure.helpers.logger")
    # @patch(
    #    "cfa_azure.blob_helpers.check_virtual_directory_existence",
    #    MagicMock(return_value=True),
    # )
    # @patch(
    #    "cfa_azure.blob_helpers.download_file", MagicMock(return_value=True)
    # )
    # def test_download_directory_extensions(self, mock_logger):
    #    blob_service_client = FakeClient()
    #    cfa_azure.blob_helpers.download_directory(
    #        container_name=FAKE_INPUT_CONTAINER,
    #        src_path="some_path/",
    #        dest_path="another_path",
    #        blob_service_client=blob_service_client,
    #        exclude_extensions=".txt",
    #        verbose=True,
    #    )
    #    mock_logger.debug.assert_called_with("Download complete.")

    # @patch("cfa_azure.helpers.logger")
    # @patch(
    #    "cfa_azure.blob_helpers.download_file", MagicMock(return_value=True)
    # )
    # def test_download_directory_extensions_inclusions(self, mock_logger):
    #    blob_service_client = FakeClient()
    #    with self.assertRaises(Exception) as exc:
    #        cfa_azure.blob_helpers.download_directory(
    #            container_name=FAKE_INPUT_CONTAINER,
    #            src_path="some_path/",
    #            dest_path="another_path",
    #            blob_service_client=blob_service_client,
    #            include_extensions=".csv",
    #            exclude_extensions=".txt",
    #            verbose=True,
    #        )
    #    mock_logger.error.assert_called_with(
    #        "Use included_extensions or exclude_extensions, not both."
    #    )
    #    self.assertEqual(
    #        "Use included_extensions or exclude_extensions, not both.",
    #        str(exc.exception),
    #    )

    @patch(
        "cfa_azure.blob_helpers.format_extensions",
        MagicMock(side_effect=(lambda x: [x[0]])),
    )
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS),
    )
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
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

    @patch(
        "cfa_azure.blob_helpers.format_extensions",
        MagicMock(side_effect=(lambda x: [x[0]])),
    )
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS),
    )
    @patch("builtins.input", MagicMock(return_value="y"))
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
    def test_upload_files_in_folder_exclusions(self):
        blob_service_client = FakeClient()
        uploaded_files = cfa_azure.blob_helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions=".txt",
            location_in_blob="",
            blob_service_client=blob_service_client,
            verbose=True,
            force_upload=True,
        )
        self.assertEqual(uploaded_files, FAKE_FOLDER_CONTENTS)

    @patch(
        "cfa_azure.blob_helpers.format_extensions",
        MagicMock(side_effect=(lambda x: [x[0]])),
    )
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS),
    )
    @patch("builtins.input", MagicMock(return_value="y"))
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
    def test_upload_files_in_folder_exclusions_forced(self):
        blob_service_client = FakeClient()
        uploaded_files = cfa_azure.blob_helpers.upload_files_in_folder(
            FAKE_FOLDER,
            FAKE_INPUT_CONTAINER,
            exclude_extensions=".txt",
            location_in_blob="",
            blob_service_client=blob_service_client,
            verbose=True,
            force_upload=False,
        )
        self.assertEqual(uploaded_files, FAKE_FOLDER_CONTENTS)

    @patch(
        "cfa_azure.blob_helpers.format_extensions",
        MagicMock(side_effect=(lambda x: [x[0]])),
    )
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=False),
    )
    @patch("os.path.isdir", MagicMock(return_value=True))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS),
    )
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
    def test_upload_files_in_folder_nonexisting(self):
        blob_service_client = FakeClient()
        with self.assertRaises(Exception) as exc:
            cfa_azure.blob_helpers.upload_files_in_folder(
                "some_folder",
                container_name=FAKE_OUTPUT_CONTAINER,
                include_extensions=[".csv"],
                location_in_blob="",
                blob_service_client=blob_service_client,
                verbose=True,
                force_upload=True,
            )
        self.assertEqual(
            f"Blob container {FAKE_OUTPUT_CONTAINER} does not exist. Please try again with an existing Blob container.",
            str(exc.exception),
        )

    @patch(
        "cfa_azure.blob_helpers.format_extensions",
        MagicMock(side_effect=(lambda x: [x[0]])),
    )
    @patch(
        "tests.fake_client.FakeClient.FakeContainerClient.exists",
        MagicMock(return_value=True),
    )
    @patch("os.path.dirname", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.path.isdir", MagicMock(return_value=False))
    @patch("os.path.realpath", MagicMock(return_value=FAKE_FOLDER))
    @patch("os.walk", MagicMock(return_value=FAKE_FOLDER_CONTENTS))
    @patch(
        "cfa_azure.blob_helpers.walk_folder",
        MagicMock(return_value=FAKE_FOLDER_CONTENTS),
    )
    @patch(
        "cfa_azure.blob_helpers.upload_blob_file", MagicMock(return_value=True)
    )
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

    def test_format_extensions(self):
        extension = "csv"
        formatted = cfa_azure.blob_helpers.format_extensions(extension)
        self.assertEqual(formatted, [".csv"])
