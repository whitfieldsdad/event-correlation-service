from dataclasses import dataclass
from typing import Iterable, Optional
from event_correlation_service.event_sinks import EventSink
from event_correlation_service.event_sources import EventSource


@dataclass()
class Service:
    event_sources: Optional[Iterable[EventSource]] = None
    event_sinks: Optional[Iterable[EventSink]] = None

    def run(self):
        raise NotImplementedError()
