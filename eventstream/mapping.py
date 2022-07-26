from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MappingRule:
    field_name: str
    action: str
    value: Any

    def validate_action(self, action: str) -> str:
        allowed_actions = ["eq", "neq", "startswith", "gt", "gte", "lt", "lte"]
        if action not in allowed_actions:
            raise ValueError("Incorrect action: %s" % action)
        return action

    def __post_init__(self) -> None:
        math_action = [
            "gt",
            "gte",
            "lt",
            "lte",
        ]
        string_action = [
            "startswith",
        ]

        if self.action in math_action and type(self.value) not in (int, float, complex):
            raise ValueError("Math action required math type")

        if self.action in string_action and type(self.value) not in (str,):
            raise ValueError("String action required string")


@dataclass
class MappingAction:
    field_name: str
    value: Any
