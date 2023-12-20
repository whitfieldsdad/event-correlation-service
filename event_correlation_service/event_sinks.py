from abc import ABC
from dataclasses import dataclass
from typing import Iterable


@dataclass()
class EventSink(ABC):
    def write_events(self, events: Iterable[dict]):
        raise NotImplementedError()
