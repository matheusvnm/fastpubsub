import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import uvicorn
import uvicorn.importer

from starconsumers.applications import StarConsumers
from starconsumers.exceptions import StarConsumersCLIException


@dataclass(frozen=True)
class ServerConfiguration:
    host: str
    port: int
    reload: bool
    root_path: str


@dataclass(frozen=True)
class AppConfiguration:
    path: str
    tasks: list[str]


class ApplicationRunner:
    def run(
        self, app_configuration: AppConfiguration, server_configuration: ServerConfiguration
    ) -> None:
        app: StarConsumers = self.get_application(app_configuration.path)

        app.activate_tasks(app_configuration.tasks)
        uvicorn.run(app=app, lifespan="on", log_level="warning", **asdict(server_configuration))

    def get_application(self, path: str) -> StarConsumers:
        posix_path = self.translate_pypath_to_posix(pypath=path)
        self.resolve_application_posix_path(posix_path=posix_path)

        app: StarConsumers = uvicorn.importer.import_from_string(path)
        if not app or not isinstance(app, StarConsumers):
            raise StarConsumersCLIException(f"The app {path} is not a {StarConsumers} instance")

        return app

    def translate_pypath_to_posix(self, pypath: str) -> Path:
        try:
            module, _ = pypath.split(":")
            posix_text_path = module.replace(".", "/")
            return Path(posix_text_path)
        except Exception as e:
            raise uvicorn.importer.ImportFromStringError(
                f'The application path "{pypath}" must be in format "<module>:<attribute>".'
            ) from e

    def resolve_application_posix_path(self, posix_path: Path) -> None:
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
