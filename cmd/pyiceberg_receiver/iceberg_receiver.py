import pyarrow as pa
import json
from pgwatch_pb2_grpc import ReceiverServicer
from pgwatch_pb2 import Reply
from google.protobuf import json_format
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    NestedField,
    StringType
)

class IcebergReceiver(ReceiverServicer):
    def __init__(self, icebergDataDir: str):
        catalog = load_catalog("pgcatalog")
        catalog.create_namespace_if_not_exists("pgwatch")

        schema = Schema(
            NestedField(field_id=1, name="DBName", field_type=StringType(), required=True),
            NestedField(field_id=2, name="MetricName", field_type=StringType(), required=True),
            NestedField(field_id=3, name="Data", field_type=StringType(), required=True),
        )

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="MetricName"
            ),
            PartitionField(
                source_id=1, field_id=1001, transform=IdentityTransform(), name="DBName"
            ),
        )

        tbl = catalog.create_table_if_not_exists(
            identifier="pgwatch.metrics",
            schema=schema,
            location=icebergDataDir,
            partition_spec=partition_spec
        )        

        self.catalog = catalog
        self.tbl = tbl
        self.arrow_schema = tbl.schema().as_arrow()


    def UpdateMeasurements(self, request, context):
        data = [json_format.MessageToDict(row) for row in request.Data]
        dataJson = json.dumps(data)

        measurement = [{
            "DBName": request.DBName,
            "MetricName": request.MetricName,
            "Data": dataJson,
        }]

        df = pa.Table.from_pylist(measurement, schema=self.arrow_schema)
        self.tbl.append(df)

        return Reply(logmsg="Metrics Inserted in iceberg.")