from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Dict, Any, List, Union


class Event(BaseModel):
    topic: str = Field(..., example="auth-service.production.login")
    event_id: str = Field(..., example="f47ac10b-58cc-4372-a567-0e02b2c3d479")
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

    @field_validator("topic")
    @classmethod
    def topic_not_empty(cls, v):
        if not v.strip():
            raise ValueError("topic tidak boleh kosong")
        return v

    @field_validator("event_id")
    @classmethod
    def event_id_not_empty(cls, v):
        if not v.strip():
            raise ValueError("event_id tidak boleh kosong")
        return v


class PublishRequest(BaseModel):
    events: Union[List[Event], Event]

    def get_events(self) -> List[Event]:
        if isinstance(self.events, Event):
            return [self.events]
        return self.events