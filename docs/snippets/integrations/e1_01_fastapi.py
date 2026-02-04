from fastapi import APIRouter
from pydantic import BaseModel, Field

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:hybrid_app]
class UserTask(BaseModel):
    user_id: int
    task_name: str


# Standard FastAPI POST endpoint
@app.post("/tasks/")
async def create_task(task: UserTask):
    """Receives an HTTP POST request and publishes to Pub/Sub."""
    await broker.publish(topic_name="tasks", data=task)
    return {"message": "Task accepted"}


# Standard FastPubSub subscriber
@broker.subscriber(
    alias="task-handler",
    topic_name="tasks",
    subscription_name="tasks-subscription",
)
async def handle_task(message: Message):
    """Consumes messages from the 'tasks' topic."""
    task = UserTask.model_validate_json(message.data)
    logger.info(f"Processing task for user {task.user_id}...")
# --8<-- [end:hybrid_app]


class MyData(BaseModel):
    value: str

# --8<-- [start:async_endpoint]
# Correct
@app.post("/submit")
async def submit_data(data: MyData):
    await broker.publish(topic_name="events", data=data)
    return {"status": "ok"}
# --8<-- [end:async_endpoint]


async def fetch_order(order_id: str) -> dict:
    return {"order_id": order_id}


async def fetch_items(order_id: str) -> list:
    return []


# --8<-- [start:path_params]
@app.get("/orders/{order_id}")
async def get_order(order_id: str, include_items: bool = False):
    order = await fetch_order(order_id)
    if include_items:
        order["items"] = await fetch_items(order_id)
    return order
# --8<-- [end:path_params]


# --8<-- [start:request_body]
class CreateOrder(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)

@app.post("/orders/")
async def create_order(order: CreateOrder):
    await broker.publish("orders", data=order)
    return {"status": "queued"}
# --8<-- [end:request_body]


# --8<-- [start:fastapi_router]
api_router = APIRouter(prefix="/api/v1")

@api_router.get("/status")
async def status():
    return {"status": "healthy"}


app.include_router(api_router)
# --8<-- [end:fastapi_router]


async def process_order(order: CreateOrder) -> str:
    return "order-123"


# --8<-- [start:response_model]
class OrderResponse(BaseModel):
    order_id: str
    status: str

@app.post("/new-orders/", response_model=OrderResponse)
async def create_new_order(order: CreateOrder):
    order_id = await process_order(order)
    return OrderResponse(order_id=order_id, status="created")
# --8<-- [end:response_model]
