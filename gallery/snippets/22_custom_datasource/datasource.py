"""A minimal custom Python DataSource (Spark 4.0+).

Referenced by blueprint.yml as `class: datasource.MyDataSource`. The module must
be importable at run time — running `aqueduct run` from this directory puts it on
the path.
"""

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class _InMemoryReader(DataSourceReader):
    def __init__(self, schema):
        self._schema = schema

    def read(self, partition):
        yield (1, "alpha")
        yield (2, "beta")
        yield (3, "gamma")


class MyDataSource(DataSource):
    @classmethod
    def name(cls):
        return "aq_inmemory"

    def schema(self):
        return StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )

    def reader(self, schema):
        return _InMemoryReader(schema)
