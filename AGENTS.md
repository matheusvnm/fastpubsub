# FastPubSub Project Context

## 1. Project Overview

**FastPubSub** is a high-performance, asynchronous framework for building Google Cloud Pub/Sub message consumers. It is designed to mirror the developer experience (DX) of **FastAPI**, providing a robust, type-safe, and intuitive API for event-driven microservices.

### Core Philosophy
*   **Familiarity:** Uses decorators (`@broker.subscriber`) and patterns (Routers, Middlewares) similar to FastAPI/Starlette.
*   **Type Safety:** Built with strict Python type hinting and **Pydantic** for data validation and configuration.
*   **Performance:** Bridges the multi-threaded Google Pub/Sub SDK with Python's `asyncio` event loop for efficient non-blocking message processing.

## 2. Architecture & Key Components

The framework acts as a high-level wrapper around the `google-cloud-pubsub` library.

*   **`FastPubSub` (Application):** The main entry point that manages the application lifecycle (startup/shutdown events) and orchestrates the broker.
*   **`PubSubBroker`:** The core component that registers subscribers and publishers. It manages the connection to the Pub/Sub backend.
*   **`Router`:** Provides modularity by allowing subscribers to be defined in separate files and included in the main broker (similar to `APIRouter` in FastAPI).
*   **`Subscriber`:** Encapsulates the execution pipeline for a subscription: receiving the message -> running middleware -> executing the user handler.
*   **`ConcurrencyManager`:** A specialized component that manages the background tasks for message pulling, ensuring graceful shutdowns and error isolation.

## 3. Building and Running

The project uses **`uv`** for dependency management and **`just`** as a command runner to standardize development tasks.

### Prerequisites
*   Python 3.12+
*   `uv` (Universal Package Manager)
*   `just` (Command Runner)
*   Google Pub/Sub Emulator (for local development)

### Development Commands

| Task | Command | Description |
| :--- | :--- | :--- |
| **Install** | `just install` | Sets up the environment and installs dependencies. |
| **Run App** | `fastpubsub run <module>:<app>` | Runs the application (e.g., `fastpubsub run main:app`). |
| **Dev Run** | `fastpubsub run ... --reload` | Runs with hot-reloading for rapid development. |
| **Test** | `just test` | Executes the test suite using `pytest`. |
| **Lint** | `just lint` | Runs `ruff` (linter) and `mypy` (type checker). |
| **Format** | `just format` | Auto-formats code using `ruff`. |
| **Coverage** | `just coverage` | Runs tests and generates a coverage report. |
| **Clean** | `just clean` | Removes build artifacts and cache files. |

## 4. Development Conventions

### Code Style
*   **Strict Typing:** `mypy` is configured in strict mode. All functions and methods must have type hints.
*   **Linting:** `ruff` is used for linting and formatting. Configuration is in `pyproject.toml`.
    *   Line length: 100 characters.
    *   Docstrings: **Google Style**.
*   **Imports:** Sorted and organized automatically by `ruff`.

### Testing Strategy
*   **Framework:** `pytest` with `pytest-asyncio` (`asyncio_mode = "auto"`).
*   **Coverage:** Minimum coverage requirement is **80%**.
*   **Mocking:** `unittest.mock` or `pytest-mock` is used to mock the underlying Google Pub/Sub client for unit tests.

### Directory Structure
*   `fastpubsub/`: Source code.
    *   `cli/`: Command-line interface implementation.
    *   `concurrency/`: Asyncio task management logic.
    *   `middlewares/`: Middleware base classes and implementations.
    *   `pubsub/`: Core Publisher and Subscriber logic.
*   `examples/`: Reference implementations for various use cases.
*   `tests/`: Unit and integration tests.

## 5. Key Concepts for Contributors

*   **Middleware:** Implements a "Chain of Responsibility" pattern. Middlewares can intercept both incoming messages (`on_message`) and outgoing publications (`on_publish`).
*   **Prefixing:** Routers support prefixes, allowing for namespaced topic/subscription management.
*   **Error Handling:** Exceptions in handlers should generally be caught by middleware or the framework to ensure messages are NACKed (negative acknowledged) or dead-lettered appropriately.
