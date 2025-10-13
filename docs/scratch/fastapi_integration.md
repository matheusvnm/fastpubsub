# FastAPI Integration

**FastPubSub** is built on top of **FastAPI**, which means you can use all the features of **FastAPI** in your **FastPubSub** applications, including dependency injection, background tasks, and more.

## The lifespan

**FastPubSub** uses the `lifespan` context manager to start and stop the message consumption process. This means that the message consumption will start when the application starts up and stop when the application shuts down.

## The Health Checks

**FastPubSub** automatically adds two routes to your application:

- `/consumers/alive`: This route can be used to check if the message consumer tasks are alive.
- `/consumers/ready`: This route can be used to check if the message consumer tasks are ready.

## Sending Messages to Background

You can use **FastAPI**'s `BackgroundTasks` to send messages to the background. This is useful if you want to send a message without blocking the current request.

Here's an example of how to send a message to the background:

```python
from fastapi import BackgroundTasks
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@app.post("/send-message")
async def send_message(background_tasks: BackgroundTasks):
    background_tasks.add_task(broker.publish, "my-topic", "Hello, world!")
    return {"message": "Message sent to the background"}
```
