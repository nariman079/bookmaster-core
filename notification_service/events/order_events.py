from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict
from abc import ABC


@dataclass
class Event(ABC):
    event_type: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
