from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from pandas import DataFrame, Series


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


class Eventstream:
    _dataset: DataFrame
    _source_schema: list[str]
    _schema: list[str]
    _removed_from_dataset: DataFrame

    __source_schema_required_fields: list[str] = ["event_timestamp", "user_id", "event_name"]
    __schema_required_fields: list[str] = ["event_timestamp", "user_id", "event"]

    @property
    def dataset(self) -> DataFrame:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: DataFrame) -> None:
        assert isinstance(dataset, DataFrame), TypeError("Pandas dataframe required")
        self._dataset = dataset
        if "event" not in self._dataset:
            self._dataset = self._dataset.assign(event="")

    @property
    def source_schema(self) -> list[str]:
        return self._source_schema

    @source_schema.setter
    def source_schema(self, source_schema: list[str]) -> None:
        assert isinstance(source_schema, list), TypeError("source_schema must be a list")
        assert all([True if x in source_schema else False for x in self.__source_schema_required_fields])
        self._source_schema = source_schema

    @property
    def schema(self) -> list[str]:
        return self._schema

    @schema.setter
    def schema(self, schema: list[str]) -> None:
        assert isinstance(schema, list), TypeError("schema must be a list")
        assert all([True if x in schema else False for x in self.__schema_required_fields])
        self._schema = schema

    def __init__(
        self,
        dataset: DataFrame,
        source_schema: list[str],
        schema: list[str],
    ) -> None:
        self.dataset = dataset
        self.schema = schema
        self.source_schema = source_schema

    def __len__(self) -> int:
        return len(self.dataset)

    def transform(self, mapping: list[dict[str, Any]], action: dict[str, Any]) -> None:
        """
        before: [{'field_name': '...', 'action': 'eq/neq/startswith', 'value': '...']],
        after:  ['field_name', 'value_name']
        """
        mapping_rules = [MappingRule(**x) for x in mapping]
        for mapping_rule in mapping_rules:
            if mapping_rule.field_name not in self.source_schema:
                raise ValueError(f"{mapping_rule.field_name} not in source_schema!")

        mapping_action = MappingAction(**action)
        if mapping_action.field_name not in self.schema:
            raise ValueError(f"{mapping_action.field_name} not in source_schema!")

        df = self._apply_filters(filters=mapping_rules, action=mapping_action)
        self.dataset = df

    def _apply_filters(self, filters: list[MappingRule], action: MappingAction | None = None) -> "DataFrame":
        criterions = [self._build_criterions(filter) for filter in filters]
        base_filter = " and ".join([x for x in criterions])
        df = self.dataset.eval(base_filter)

        if action:
            self.dataset.loc[self.dataset.eval(base_filter), "event"] = action.value
            return self.dataset

        return df

    def _build_criterions(self, filter: MappingRule) -> str | Series:
        if filter.action == "startswith":
            criterion = f'@self.dataset["{filter.field_name}"].str.startswith("{filter.value}")'
            return criterion

        actions = {
            "eq": "==",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
        }
        query = f'`{filter.field_name}` {actions[filter.action]} "{filter.value}"'
        return query

    def remove_rows(self, remove_rules: list[dict[str, Any]]) -> "DataFrame":
        filters = [MappingRule(**x) for x in remove_rules]
        for remove_rule in filters:
            if remove_rule.field_name not in self.source_schema:
                raise ValueError(f"{remove_rule.field_name} not in source_schema!")

        df = self._apply_filters(filters)

        self._removed_from_dataset = self.dataset[df]
        self.dataset = self.dataset[~df]

    def restore_removed_rows(self) -> None:
        self.dataset = pd.concat([self.dataset, self._removed_from_dataset], ignore_index=True)

    def get_dataframe(self) -> "DataFrame":
        return self.dataset[self.schema]
