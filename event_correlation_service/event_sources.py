from abc import ABC
from dataclasses import dataclass
from typing import Iterable


@dataclass()
class EventSource(ABC):
    def read_events(self, events: Iterable[dict]):
        raise NotImplementedError()
