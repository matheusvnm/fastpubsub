from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:alerting_middleware]
class AlertingMiddleware(BaseMiddleware):
    def __init__(self, next_call: BaseMiddleware, error_threshold: int = 10):
        super().__init__(next_call)
        self.error_threshold = error_threshold
        self.error_count = 0
        self.alert_sent = False

    async def on_message(self, message: Message):
        try:
            return await super().on_message(message)
        except Exception:
            self.error_count += 1

            if self.error_count >= self.error_threshold and not self.alert_sent:
                await self.send_alert(f"High error rate: {self.error_count} errors")
                self.alert_sent = True

            raise

    async def send_alert(self, message: str):
        logger.critical(f"ALERT: {message}")
        # Send to PagerDuty, Slack, etc.


broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(AlertingMiddleware, error_threshold=10)],
)
# --8<-- [end:alerting_middleware]

app = FastPubSub(broker)


async def send_alert_to_ops(severity: str, message: str, context: dict) -> None:
    """Send alert to operations team."""
    pass


# --8<-- [start:dlq_monitoring]
@broker.subscriber(
    alias="dlq-monitor",
    topic_name="orders-dlq",
    subscription_name="dlq-monitor-subscription",
)
async def monitor_dead_letters(message: Message):
    logger.critical(
        "Message sent to DLQ",
        extra={
            "message_id": message.id,
            "original_topic": message.attributes.get("original_topic"),
            "failure_count": message.attributes.get("delivery_attempt"),
        },
    )

    await send_alert_to_ops(
        severity="high",
        message=f"DLQ message: {message.id}",
        context=message.attributes,
    )


# --8<-- [end:dlq_monitoring]
