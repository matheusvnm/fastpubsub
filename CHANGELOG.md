## 0.1.0 (2025-10-25)

### Feat

- adds autoreviving workers and liveness/readiness/info routes
- adds monitoring process for healthcheck
- adds gzip middleware and basic observability built-in
- adds some default middlewares
- add subrouters hierarchy
- add subrouters hierarchy
- massive refactoring in the project make it runnable
- massive refactoring in the project make it runnable
- adds support for FastAPI integration
- adds support for FastAPI integration
- adds autocreation feature for topic publishing
- first refactor model
- adds new commands for send messages and creating topics locally
- adds subscription autoupdate
- adding new control flow, retry, autoupdate policies and local middlewares
- adds new precommit hooks
- adds handler argument validation
- add quality tools and type checking
- adds the logger to all the project
- adds some core logger logic
- adds middleware and apm provider abstractions
- adds middleware support
- adds instrumentation utils
- adds the first functional version of the library
- adds the logic to initiate/select the tasks
- adds the base structure of the project

### Fix

- fixes contextualize data-race condition and pull cancellation
- fixes unit testing for new concurrency model using lightweight async tasks
- issues related to lint
- updated dependencies, fixes opened grpc conn and adds shutdown method of the observability agents
- minor liveness fix and making the examples work again
- lint issues related to testing
- some lint issues
- fixes the type hints
- start typing fixes and adds justfile
- publish command async execution
- adds some typecheckings
- middleware/router add order
- middleware ordering and publisher example
- starts middleware for publishers implementation
- resolver probes problems and remove hotreload
- application path discovery
- application path discovery
- some type checking issues and formating
- type hints even more
- type hints
- some typos
- turns log level into severity

### Refactor

- alters the consumption loop to become more testable
- concurrency model using asyncio
- refactor some tests to avoid repetition and make them clear
- changes starconsumers reference
- standarlize precommits names
- starts files reorganization
