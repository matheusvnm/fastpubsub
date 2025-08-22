

import os

from starconsumers.exceptions import StarConsumersException


def check_credentials():
       credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
       emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
       if not credentials and not emulator_host:
            raise StarConsumersException("You should set either of the environment variables for authentication:" \
            " (GOOGLE_APPLICATION_CREDENTIALS, PUBSUB_EMULATOR_HOST)")

