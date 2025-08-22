from dataclasses import asdict, dataclass

import uvicorn
import uvicorn.importer

from starconsumers.applications import StarConsumers
from starconsumers.cli.discover import Application


@dataclass
class ServerConfiguration:
    host: str
    port: int
    reload: bool
    root_path: str
    tasks: list[str] | None


class ApplicationRunner:
    def run(self, application: Application, configuration: ServerConfiguration) -> None:
        app: StarConsumers = uvicorn.importer.import_from_string(str(application))

        server_configuration = asdict(configuration)
        app.activate_tasks(server_configuration.pop("tasks"))

        uvicorn.run(app=app, lifespan="on", log_level="warning", **server_configuration)
