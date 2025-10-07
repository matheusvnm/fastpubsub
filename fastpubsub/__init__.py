"""A high performance FastAPI-based message consumer framework for PubSub"""

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter

__all__ = [
    "FastPubSub",
    "PubSubBroker",
    "PubSubRouter",
    "Publisher",
    "Subscriber",
    "BaseMiddleware",
    "Message",
]

# TODO: Adicionar controle de fluxo no poll.
# TODO: Adicionar configuração segregada no subscriber e passar ela para o client de conexão.
# TODO: Checar todo o sistema de exemplos.
# TODO: Reaplicar o lint no projeto.
# TODO: Reestruturar os testes unitários.
