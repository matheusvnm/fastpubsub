"""Simple and fast framework to create message brokers based microservices."""

from importlib.metadata import version

__version__ = version("fastpubsub")

SERVICE_NAME = f"fastpubsub-{__version__}"
