import os
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest
import uvicorn.importer
from typer.testing import CliRunner

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.cli.main import app
from fastpubsub.cli.runner import AppConfiguration, ApplicationRunner, ServerConfiguration
from fastpubsub.cli.utils import LogLevels, get_log_level
from fastpubsub.exceptions import FastPubSubCLIException

runner = CliRunner()


class TestCLI:
    def test_main_no_command(self):
        result = runner.invoke(app)
        assert result.exit_code == 0
        assert "Welcome to the FastPubSub CLI!" in result.stdout
        assert "Common Commands:" in result.stdout

    def test_help_command(self):
        result = runner.invoke(app, ["help"])
        assert result.exit_code == 0
        assert "Usage: fastpubsub" in result.stdout

    def test_version_option(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "Running FastStream" in result.stdout

    @patch("fastpubsub.cli.main.ensure_pubsub_credentials")
    @patch("fastpubsub.cli.main.ApplicationRunner")
    def test_run_command(self, mock_runner: MagicMock, mock_ensure_credentials: MagicMock):
        result = runner.invoke(app, ["run", "some_module:app"])
        assert result.exit_code == 0
        mock_runner.assert_called_once()
        mock_ensure_credentials.assert_called_once()

    @patch("fastpubsub.cli.main.ensure_pubsub_credentials")
    @patch("fastpubsub.cli.main.ApplicationRunner")
    def test_run_command_with_options(
        self, mock_runner_class: MagicMock, mock_ensure_credentials: MagicMock
    ):
        mock_runner_instance = mock_runner_class.return_value
        result = runner.invoke(
            app,
            [
                "run",
                "some_module:app",
                "--workers",
                "2",
                "--port",
                "8001",
                "--host",
                "127.0.0.1",
                "--reload",
                "--log-level",
                "debug",
                "--server-log-level",
                "error",
                "--subscribers",
                "subscriber1",
                "--subscribers",
                "subscriber2",
            ],
        )
        assert result.exit_code == 0
        mock_ensure_credentials.assert_called_once()

        expected_app_config = AppConfiguration(
            app="some_module:app",
            log_level=10,  # debug
            log_serialize=False,
            log_colorize=False,
            apm_provider="NOOP",
            subscribers={"subscriber1", "subscriber2"},
        )
        expected_server_config = ServerConfiguration(
            host="127.0.0.1",
            port=8001,
            workers=2,
            reload=True,
            log_level=40,  # error
        )

        mock_runner_instance.run.assert_called_once_with(
            expected_app_config, expected_server_config
        )


class TestApplicationRunner:
    @patch("uvicorn.run")
    @patch("fastpubsub.cli.runner.ApplicationRunner.validate_application")
    def test_run(self, mock_validate: MagicMock, mock_uvicorn_run: MagicMock):
        app_config = AppConfiguration(
            app="my_app:app",
            log_level=10,
            log_serialize=False,
            log_colorize=True,
            apm_provider="NOOP",
            subscribers=set(),
        )
        server_config = ServerConfiguration(
            host="localhost",
            port=8000,
            workers=1,
            reload=False,
            log_level=20,
        )

        runner_instance = ApplicationRunner()
        runner_instance.run(app_config, server_config)

        mock_validate.assert_called_once_with(app_config.app)
        mock_uvicorn_run.assert_called_once_with(
            app_config.app, lifespan="on", **asdict(server_config)
        )

    @patch("fastpubsub.cli.runner.ApplicationRunner.resolve_application_posix_path")
    @patch("fastpubsub.cli.runner.ApplicationRunner.translate_pypath_to_posix")
    @patch("uvicorn.importer.import_from_string")
    def test_validate_application_invalid_app_instance(
        self, mock_import: MagicMock, mock_translate: MagicMock, mock_resolve: MagicMock
    ):
        mock_import.return_value = object()
        runner_instance = ApplicationRunner()
        with pytest.raises(FastPubSubCLIException):
            runner_instance.validate_application("my_app:app")

    @patch("fastpubsub.cli.runner.ApplicationRunner.resolve_application_posix_path")
    @patch("fastpubsub.cli.runner.ApplicationRunner.translate_pypath_to_posix")
    @patch("uvicorn.importer.import_from_string")
    def test_validate_application_valid_app(
        self, mock_import: MagicMock, mock_translate: MagicMock, mock_resolve: MagicMock
    ):
        mock_broker = MagicMock(spec=PubSubBroker)
        mock_import.return_value = FastPubSub(broker=mock_broker)
        runner_instance = ApplicationRunner()
        # should not raise
        runner_instance.validate_application("my_app:app")

    def test_translate_pypath_to_posix_invalid_format(self):
        runner_instance = ApplicationRunner()
        with pytest.raises(uvicorn.importer.ImportFromStringError):
            runner_instance.translate_pypath_to_posix("invalid_path")


class TestUtils:
    def test_get_log_level(self):
        assert get_log_level(LogLevels.CRITICAL) == 50
        assert get_log_level("ERROR") == 40
        assert get_log_level(30) == 30

        with pytest.raises(FastPubSubCLIException):
            get_log_level("invalid_level")

    def test_ensure_pubsub_credentials_set(self):
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "fake_credentials"}):
            from fastpubsub.cli.utils import ensure_pubsub_credentials

            ensure_pubsub_credentials()

        with patch.dict(os.environ, {"PUBSUB_EMULATOR_HOST": "localhost"}):
            from fastpubsub.cli.utils import ensure_pubsub_credentials

            ensure_pubsub_credentials()

    def test_ensure_pubsub_credentials_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            from fastpubsub.cli.utils import ensure_pubsub_credentials

            with pytest.raises(FastPubSubCLIException):
                ensure_pubsub_credentials()
