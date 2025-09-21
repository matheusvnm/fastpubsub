from dataclasses import dataclass, field


class InterprocessRequest:
    """Base class for all inter-process requests."""

    pass


@dataclass(frozen=True)
class InterprocessResponse:
    """Base class for all inter-process responses."""

    pass


@dataclass(frozen=True)
class LivenessProbeRequest(InterprocessRequest):
    """Request to check if the subprocesses are running."""

    timeout: float


@dataclass(frozen=True)
class LivenessProbeResponse(InterprocessResponse):
    """Response containing the liveness results."""

    results: dict[str, bool]


@dataclass(frozen=True)
class ReadinessProbeRequest(InterprocessRequest):
    """Request to check the operational status and readiness of subscribers."""

    pass


@dataclass(frozen=True)
class ReadinessProbeResponse(InterprocessResponse):
    """Response containing the readiness status of all subscribers."""

    results: dict[str, bool]


@dataclass(frozen=True)
class InfoRequest(InterprocessRequest):
    """Request to gather detailed information about all processes."""

    pass


@dataclass(frozen=True)
class ConnectionInfo:
    """Detailed information about a single network connection."""

    remote_address: str
    remote_port: int
    hostname: str
    status: str


@dataclass(frozen=True)
class ProcessInfo:
    """Detailed information about a single OS process."""

    pid: int
    running: bool
    num_threads: int
    connections: list[ConnectionInfo] = field(default_factory=list)


@dataclass(frozen=True)
class SubscriberInfo:
    """Aggregated information about a SubscriberWorker."""

    process_info: ProcessInfo


@dataclass(frozen=True)
class WorkerInfo:
    """Aggregated information about a ManagerWorker and its children."""

    process_info: ProcessInfo
    subscribers: dict[str, SubscriberInfo] = field(default_factory=dict)


@dataclass(frozen=True)
class InfoResponse(InterprocessResponse):
    """Response containing detailed information about the manager and its subscribers."""

    worker_info: WorkerInfo
