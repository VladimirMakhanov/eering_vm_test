from typing import TYPE_CHECKING

import pandas as pd
import pytest

if TYPE_CHECKING:
    from pandas._typing import DataFrame

from eventstream import Eventstream
from eventstream.tools import generate_clickstream


class TestCreateEventStream:
    def test_correct_create_event_stream(self) -> None:
        cs = generate_clickstream(rows=10)
        es = Eventstream(dataset=cs, source_schema=list(cs.keys()), schema=["event_timestamp", "user_id", "event"])
        assert len(es) == 10

    def test_wrong_dataset_type(self) -> None:
        cs: "DataFrame" = pd.DataFrame({"event_name": ["aaa", "bbb"]})
        with pytest.raises(AssertionError):
            Eventstream(dataset=cs, source_schema=list(cs.keys()), schema=["event_timestamp", "user_id", "event"])

    def test_wrong_source_schema(self) -> None:
        cs = generate_clickstream(rows=10)
        with pytest.raises(AssertionError):
            Eventstream(dataset=cs, source_schema=["user_id"], schema=["event_timestamp", "user_id", "event"])

    def test_wrong_schema(self) -> None:
        cs = generate_clickstream(rows=10)
        with pytest.raises(AssertionError):
            Eventstream(
                dataset=cs,
                source_schema=list(cs.keys()),
                schema=[
                    "event_timestamp",
                    "user_id",
                ],
            )
