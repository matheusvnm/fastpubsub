import importlib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from starconsumers.applications import StarConsumers
from starconsumers.exceptions import StarConsumersCLIException
from starconsumers.logger import logger


@dataclass
class Module:
    path: str
    directory: Path


@dataclass
class Application:
    name: str
    module: Module

    def __str__(self) -> str:
        return f"{self.module.path}:{self.name}"


def get_default_path() -> Path:
    potential_paths = (
        "main.py",
        "app.py",
        "api.py",
        "app/main.py",
        "app/app.py",
        "app/api.py",
    )

    for full_path in potential_paths:
        path = Path(full_path)
        if path.is_file():
            return path

    raise StarConsumersCLIException(
        "Could not find a default file to run, please provide an explicit path"
    )


class ApplicationDiscover:
    def _get_default_path(self) -> Path:
        potential_dirs = (".", "app", "src", "src/api")

        potential_files = (
            "main.py",
            "app.py",
            "api.py",
        )

        for dir in potential_dirs:
            for file in potential_files:
                full_path = "/".join([dir, file])
                path = Path(full_path)
                if path.is_file():
                    return path

        raise StarConsumersCLIException(
            "Could not find a default file to run, please provide an explicit path"
        )

    def _get_module(self, *, path: Path) -> Module:
        modules = []

        absolute_path = path.resolve()
        if absolute_path.is_file() and absolute_path.stem == "__init__":
            absolute_path = absolute_path.parent
        modules.append(absolute_path)

        directory = absolute_path.parent
        for parent in absolute_path.parents:
            init_path = parent / "__init__.py"
            if not init_path.is_file():
                break

            modules.insert(0, parent)
            directory = parent.parent

        module_path = ".".join([p.stem for p in modules])
        return Module(path=module_path, directory=directory.resolve())

    def _import_module(self, *, module: Module) -> ModuleType:
        try:
            return importlib.import_module(module.path)
        except (ImportError, ValueError) as e:
            logger.error(f"Import error: {e}")
            logger.error("Ensure all the package directories have an __init__.py file")
            raise

    def _search_probable_names(self, *, module: Module) -> str:
        loaded_module = self._import_module(module=module)
        object_names = dir(loaded_module)

        for preferred_name in ["app", "api"]:
            if preferred_name in set(object_names):
                obj = getattr(loaded_module, preferred_name)
                if isinstance(obj, StarConsumers):
                    return preferred_name

        for name in object_names:
            obj = getattr(loaded_module, name)
            if isinstance(obj, StarConsumers):
                return name

        raise StarConsumersCLIException(
            "Could not find StarConsumers app in modules, try using --app_name"
        )

    def _app_name_valid(self, *, app_name: str, module: Module) -> bool:
        loaded_module = self._import_module(module=module)
        object_names = dir(loaded_module)

        if app_name not in set(object_names):
            logger.debug(f"Could not find app name {app_name} in {module.path}")
            return False

        app = getattr(loaded_module, app_name)
        if not isinstance(app, StarConsumers):
            logger.debug(
                f"The app name {app_name} in {module.path} doesn't seem to be a StarConsumers app"
            )
            return False

        return True

    def _get_app_name(self, *, module: Module, app_name: str | None = None) -> str:
        if not app_name:
            return self._search_probable_names(module=module)

        if not self._app_name_valid(app_name=app_name, module=module):
            raise StarConsumersCLIException(
                f"The app name {app_name} was not found in the directory, try using --app_name"
            )

        return app_name

    def search_application(
        self, *, path: Path | None = None, app_name: str | None = None
    ) -> Application:
        if not path:
            logger.debug(f"Using path default {path}")
            path = self._get_default_path()

        logger.debug("Searching for package file structure from directories with __init__.py files")

        logger.debug(f"Resolved absolute path {path.resolve()}")
        if not path.exists():
            raise StarConsumersCLIException(f"Path does not exist {path}")

        module = self._get_module(path=path)

        logger.debug(f"Importing module {module.path}")
        logger.debug(f"Importing from {module.directory}")

        name = self._get_app_name(module=module, app_name=app_name)
        application = Application(name=name, module=module)

        logger.info("The Starconsumers application was found")
        logger.info(f"from {application.module.path} import {application.name}")
        return application
