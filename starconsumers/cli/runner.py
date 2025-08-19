from dataclasses import asdict, dataclass

import uvicorn
import uvicorn.importer

from starconsumers.application import StarConsumers
from starconsumers.cli.discover import Application


@dataclass
class ServerConfiguration:
    host: str
    port: int
    reload: bool
    root_path: str
    proxy_headers: dict
    tasks: list[str]


class ApplicationRunner:
    def run(self, application: Application,  configuration: ServerConfiguration):
        app: StarConsumers = uvicorn.importer.import_from_string(str(application))

        server_configuration = asdict(configuration)
        app.activate_tasks(server_configuration.pop("tasks"))

        #if configuration.reload:
        #    app = str(application)

        uvicorn.run(app=app, lifespan="on", log_level="debug", **server_configuration)