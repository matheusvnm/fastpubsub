"""Application runner."""

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import uvicorn
import uvicorn.importer

from fastpubsub.logger import configure

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServerConfiguration:
    """Server configuration."""

    host: str
    port: int
    workers: int
    reload: bool
    log_level: str


@dataclass(frozen=True)
class AppConfiguration:
    """Application configuration."""

    app: str
    log_level: str
    log_serialize: bool
    log_colorize: bool
    subscribers: set[str] = field(default_factory=set)


class ApplicationRunner:
    """Runs a FastPubSub application."""

    def __init__(self, app_config: AppConfiguration, server_config: ServerConfiguration) -> None:
        """Initialized a FastPubSub application runner.

        Args:
            app_config: The application configuration.
            server_config: The server configuration.
        """
        self.app_config = app_config
        self.server_config = server_config

    def setup(self) -> None:
        """Setup a FastPubSub application environment."""
        os.environ["FASTPUBSUB_LOG_LEVEL"] = self.app_config.log_level
        os.environ["FASTPUBSUB_ENABLE_LOG_SERIALIZE"] = str(int(self.app_config.log_serialize))
        os.environ["FASTPUBSUB_ENABLE_LOG_COLORS"] = str(int(self.app_config.log_colorize))
        os.environ["FASTPUBSUB_SUBSCRIBERS"] = ",".join(self.app_config.subscribers)
        configure()

    def validate(self) -> None:
        """Validates a FastPubSub application."""
        from fastpubsub.applications import FastPubSub
        from fastpubsub.exceptions import FastPubSubCLIException

        posix_path = self._translate_pypath_to_posix(pypath=self.app_config.app)
        self._resolve_application_posix_path(posix_path=posix_path)

        app = uvicorn.importer.import_from_string(self.app_config.app)
        if not app or not isinstance(app, FastPubSub):
            raise FastPubSubCLIException(
                f"The app {self.app_config.app} is not a {FastPubSub} instance"
            )

    def run(self) -> None:
        """Runs a FastPubSub application."""
        logger.info("FastPubSub app starting...")
        uvicorn.run(
            self.app_config.app,
            lifespan="on",
            log_level=self.server_config.log_level,
            host=self.server_config.host,
            port=self.server_config.port,
            workers=self.server_config.workers,
            reload=self.server_config.reload,
        )
        logger.info("FastPubSub app terminated.")

    def _translate_pypath_to_posix(self, pypath: str) -> Path:
        try:
            module, _ = pypath.split(os.path.pathsep)
            posix_text_path = module.replace(os.path.extsep, os.path.sep)
            return Path(posix_text_path)
        except Exception as e:
            raise uvicorn.importer.ImportFromStringError(
                f'The application path "{pypath}" must be in format "<module>:<attribute>".'
            ) from e

    def _resolve_application_posix_path(self, posix_path: Path) -> None:
        module_path = posix_path.resolve()
        if module_path.is_file() and module_path.stem == "__init__":
            module_path = module_path.parent

        extra_sys_path = module_path.parent
        for parent in module_path.parents:
            init_path = parent / "__init__.py"
            if not init_path.is_file():
                break

            extra_sys_path = parent.parent

        current_directory = os.getcwd()
        sys.path.insert(0, current_directory)
        sys.path.insert(0, str(extra_sys_path))
