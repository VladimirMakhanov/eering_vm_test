from tests.tools.clickstream import generate_clickstream


class TestCreateClickstream:
    def test_create_clickstream_10_rows(self) -> None:
        stream = generate_clickstream(rows=10)
        assert len(stream) == 10

    def test_create_clickstream_50_rows(self) -> None:
        stream = generate_clickstream(rows=50)
        assert len(stream) == 50

    def test_create_clickstream_0_rows(self) -> None:
        stream = generate_clickstream(rows=0)
        assert len(stream) == 0
