from pandas import DataFrame


class Eventstream:
    _dataset: DataFrame
    _source_schema: list[str]
    _schema: list[str]

    __source_schema_required_fields: list[str] = ["event_timestamp", "user_id", "event_name"]
    __schema_required_fields: list[str] = ["event_timestamp", "user_id", "event"]

    @property
    def dataset(self) -> DataFrame:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: DataFrame) -> None:
        assert isinstance(dataset, DataFrame), TypeError("Pandas dataframe required")
        self._dataset = dataset

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
