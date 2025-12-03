"""Application runner."""

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import uvicorn
import uvicorn.importer

from fastpubsub.applications import FastPubSub
from fastpubsub.exceptions import FastPubSubCLIException
from fastpubsub.logger import logger, setup_logger


@dataclass(frozen=True)
class ServerConfiguration:
    """Server configuration."""

    host: str
    port: int
    workers: int
    reload: bool
    log_level: int


@dataclass(frozen=True)
class AppConfiguration:
    """Application configuration."""

    app: str
    log_level: int
    log_serialize: bool
    log_colorize: bool
    subscribers: set[str] = field(default_factory=set)


class ApplicationRunner:
    """Runs a FastPubSub application."""

    def run(self, app_config: AppConfiguration, server_config: ServerConfiguration) -> None:
        """Runs a FastPubSub application.

        Args:
            app_config: The application configuration.
            server_config: The server configuration.
        """
        self._setup_enviroment(app_config=app_config)

        setup_logger()

        self._validate_application(app_config.app)

        logger.info("FastPubSub app starting...")
        uvicorn.run(
            app_config.app,
            lifespan="on",
            log_level=server_config.log_level,
            host=server_config.host,
            port=server_config.port,
            workers=server_config.workers,
            reload=server_config.reload,
        )
        logger.info("FastPubSub app terminated.")

    def _setup_enviroment(self, app_config: AppConfiguration) -> None:
        os.environ["FASTPUBSUB_LOG_LEVEL"] = str(app_config.log_level)
        os.environ["FASTPUBSUB_ENABLE_LOG_SERIALIZE"] = (
            str(1) if app_config.log_serialize else str(0)
        )
        os.environ["FASTPUBSUB_ENABLE_LOG_COLORS"] = str(1) if app_config.log_colorize else str(0)
        os.environ["FASTPUBSUB_SUBSCRIBERS"] = ",".join(app_config.subscribers)

    def _validate_application(self, path: str) -> None:
        posix_path = self._translate_pypath_to_posix(pypath=path)
        self._resolve_application_posix_path(posix_path=posix_path)

        app = uvicorn.importer.import_from_string(path)
        if not app or not isinstance(app, FastPubSub):
            raise FastPubSubCLIException(f"The app {path} is not a {FastPubSub} instance")

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
