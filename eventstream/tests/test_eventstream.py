from eventstream import Eventstream
from eventstream.tools import generate_clickstream


class TestCreateEventStream:
    def test_correct_create_event_stream(self) -> None:
        cs = generate_clickstream(rows=10)
        es = Eventstream(dataset=cs, source_schema=list(cs.keys()), schema=["event_timestamp", "user_id", "event"])
        assert len(es) == 10
