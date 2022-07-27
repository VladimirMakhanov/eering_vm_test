from typing import TYPE_CHECKING

import pandas as pd
import pytest

if TYPE_CHECKING:
    from pandas._typing import DataFrame

from eventstream import Eventstream
from tests.tools import clickstream_data, generate_clickstream


class TestCreateEventStream:
    def test_correct_create_event_stream(self) -> None:
        cs = generate_clickstream(rows=10)
        es = Eventstream(dataset=cs, source_schema=list(cs.keys()), schema=["event_timestamp", "user_id", "event"])
        assert len(es) == 10

    def test_wrong_dataset_type(self) -> None:
        cs: "DataFrame" = pd.DataFrame({"event_name": ["aaa", "bbb"]})
        with pytest.raises(ValueError):
            Eventstream(dataset=cs, source_schema=list(cs.keys()), schema=["event_timestamp", "user_id", "event"])

    def test_wrong_source_schema(self) -> None:
        cs = generate_clickstream(rows=10)
        with pytest.raises(ValueError):
            Eventstream(dataset=cs, source_schema=["user_id"], schema=["event_timestamp", "user_id", "event"])

    def test_wrong_schema(self) -> None:
        cs = generate_clickstream(rows=10)
        with pytest.raises(ValueError):
            Eventstream(
                dataset=cs,
                source_schema=list(cs.keys()),
                schema=[
                    "event_timestamp",
                    "user_id",
                ],
            )


class TestMappingAndRemoveEventStream:
    @pytest.mark.usefixtures("clickstream_data")
    def test_simple_correct_mapping(self, clickstream_data: dict) -> None:
        data = pd.DataFrame(data=clickstream_data)
        es = Eventstream(dataset=data, source_schema=list(data.keys()), schema=["event_timestamp", "user_id", "event"])
        event_filter = [
            {"field_name": "device", "action": "eq", "value": "mobile"},
            {"field_name": "utm_medium", "action": "eq", "value": "social"},
        ]
        event_action = {"field_name": "event", "value": "mobile social"}
        es.transform(mapping=event_filter, action=event_action)
        assert isinstance(es.get_dataframe(), pd.DataFrame), "Not correct type"
        assert len(es.get_dataframe()) == 15, "Incorrect length of dataframe"
        assert (
            len(es.get_dataframe().loc[es.get_dataframe()["event"] == "mobile social"]) == 6
        ), "Not all rows was affected"

    @pytest.mark.usefixtures("clickstream_data")
    def test_incorrect_filter_name(self, clickstream_data: dict) -> None:
        data = pd.DataFrame(data=clickstream_data)
        es = Eventstream(dataset=data, source_schema=list(data.keys()), schema=["event_timestamp", "user_id", "event"])
        event_filter = [
            {"field_name": "device111", "action": "eq", "value": "mobile"},
            {"field_name": "utm_medium", "action": "eq", "value": "social"},
        ]
        event_action = {"field_name": "event", "value": "mobile social"}
        with pytest.raises(ValueError):
            es.transform(mapping=event_filter, action=event_action)

    @pytest.mark.usefixtures("clickstream_data")
    def test_incorrect_filter_operation(self, clickstream_data: dict) -> None:
        data = pd.DataFrame(data=clickstream_data)
        es = Eventstream(dataset=data, source_schema=list(data.keys()), schema=["event_timestamp", "user_id", "event"])
        event_filter = [
            {"field_name": "device", "action": "gte", "value": "catalog"},
            {"field_name": "utm_medium", "action": "eq", "value": "social"},
        ]
        event_action = {"field_name": "event", "value": "mobile social"}
        with pytest.raises(ValueError):
            es.transform(mapping=event_filter, action=event_action)

    @pytest.mark.usefixtures("clickstream_data")
    def test_startswith(self, clickstream_data: dict) -> None:
        data = pd.DataFrame(data=clickstream_data)
        es = Eventstream(dataset=data, source_schema=list(data.keys()), schema=["event_timestamp", "user_id", "event"])
        event_filter = [{"field_name": "page", "action": "startswith", "value": "catalog"}]
        event_action = {"field_name": "event", "value": "catalog"}
        es.transform(mapping=event_filter, action=event_action)
        assert len(es.get_dataframe()) == 15, "Incorrect length of dataframe"
        assert len(es.get_dataframe().loc[es.get_dataframe()["event"] == "catalog"]) == 5, "Not all rows was affected"

    @pytest.mark.usefixtures("clickstream_data")
    def test_remove_rows(self, clickstream_data: dict) -> None:
        data = pd.DataFrame(data=clickstream_data)
        es = Eventstream(dataset=data, source_schema=list(data.keys()), schema=["event_timestamp", "user_id", "event"])
        remove_filter = [{"field_name": "page", "action": "startswith", "value": "catalog"}]
        es.remove_rows(remove_rules=remove_filter)
        assert len(es.get_dataframe()) == 10, "Not all rows was removed"
        es.restore_removed_rows()
        assert len(es.get_dataframe()) == 15, "Not all rows was restored"
