---
icon: lucide/plug
---

# FastAPI Integration

The most important concept to understand is that `FastPubSub` **is** a FastAPI application. It inherits directly from `fastapi.FastAPI` class, which means you get all the features of a modern web framework out of the box.

This design lets you build applications that both consume Pub/Sub messages and serve a REST API in the same process. You can use FastAPI's dependency injection, routers, path operations, and more.

## Hybrid Application Example

A common pattern is having a subscriber that processes data and an API endpoint that triggers tasks:

```python
--8<-- "integrations/e1_01_fastapi.py:hybrid_app"
```

---

## Step-by-Step

1. Create a `PubSubBroker`.
2. Instantiate `FastPubSub` with the broker.
3. Add HTTP endpoints as usual.
4. Add subscribers with `@broker.subscriber`.

---

## Async Is Required

The `PubSubBroker` is fully asynchronous. Methods like `broker.publish()` are async and must be awaited.

If you want to call a broker method from a FastAPI endpoint, that endpoint must be `async def`:

```python
--8<-- "integrations/e1_01_fastapi.py:async_endpoint"

# Wrong - cannot await in sync function
# @app.post("/submit")
# def submit_data_sync(data: MyData):
#     await broker.publish(topic_name="events", data=data)  # SyntaxError!
```

---

## Boundaries: FastAPI vs FastPubSub

Two systems run in parallel: FastAPI for HTTP and FastPubSub for Pub/Sub. Understanding their boundaries is crucial.

### Middlewares

The two systems have **separate** middleware pipelines:

| System | Intercepts | Use For |
|--------|------------|---------|
| FastAPI/Starlette Middlewares | HTTP requests/responses | Authorization headers, logging HTTP requests, `RequestValidationError` |
| FastPubSub Middlewares | Pub/Sub messages | Message validation, instrumentation, custom exception handling |

You **cannot** use a FastAPI middleware to intercept a Pub/Sub message, and you cannot use a FastPubSub middleware to intercept an HTTP request.

### Exception Handlers

Errors in `@broker.subscriber` functions are **not** caught by FastAPI's `@app.exception_handler`. Each system handles its own exceptions.

### Dependency Injection

FastAPI's `Depends` is an **HTTP-only** feature:

- Works for API endpoints (`@app.get()`, `@app.post()`, etc.)
- Does **not** apply to `@broker.subscriber` handlers

The message-handling system doesn't use FastAPI's dependency injection.

---

## Health Check Endpoints

FastPubSub automatically adds two HTTP endpoints for liveness and readiness probes:

| Endpoint | Purpose |
|----------|---------|
| `/consumers/alive` | Liveness probe - checks if the web server is running |
| `/consumers/ready` | Readiness probe - checks if subscribers are actively polling |

### Customizing Paths

```python
app = FastPubSub(
    broker,
    liveness_path="/healthz",
    readiness_path="/readyz",
)
```

### Using with Kubernetes

```yaml
livenessProbe:
  httpGet:
    path: /consumers/alive
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 5

readinessProbe:
  httpGet:
    path: /consumers/ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 3
```

These endpoints respect FastAPI's prefix structure, so they'll include any `root_path` you configure.

---

## Using FastAPI Features

Since `FastPubSub` inherits from `FastAPI`, you can use all standard features:

### Path Parameters and Query Parameters

```python
--8<-- "integrations/e1_01_fastapi.py:path_params"
```

### Request Body Validation

```python
--8<-- "integrations/e1_01_fastapi.py:request_body"
```

### FastAPI Routers

```python
--8<-- "integrations/e1_01_fastapi.py:fastapi_router"
```

### Response Models

```python
--8<-- "integrations/e1_01_fastapi.py:response_model"
```

---

## Current Limitations

### API-Only Mode

FastPubSub currently requires at least one `@broker.subscriber` to be defined. An "API-only" mode (serving only HTTP endpoints) is planned but not yet supported.

Tracked in [GitHub Issue #13](https://github.com/matheusvnm/fastpubsub/issues/13).

### Handler Parameter Annotations

FastAPI-style parameter annotations for subscriber handlers aren't yet supported. The only parameter is the `message: Message` object. You must parse and validate message data manually.

Tracked in [GitHub Issue #14](https://github.com/matheusvnm/fastpubsub/issues/14).


---

## Recap

- **FastPubSub is FastAPI**: Supports all FastAPI features like routers, path operations, and response models.
- **Async required**: API endpoints calling broker methods must be `async def`.
- **Separate systems**: FastAPI Middlewares, Exception Handlers, and Dependency Injection only apply to HTTP endpoints, not Pub/Sub handlers.
- **Health checks**: Built-in `/consumers/alive` and `/consumers/ready` endpoints (customizable).
- **Limitations**: Requires at least one subscriber; FastAPI-style parameter annotations not yet supported in subscribers.
