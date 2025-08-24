from pgwatch_pb2_grpc import ReceiverServicer
from pgwatch_pb2 import Reply
import pyarrow as pa
from pyiceberg.table import Table
from google.protobuf import json_format
import json

class Receiver(ReceiverServicer):
    def __init__(self, tbl: Table):
        self.tbl = tbl
        self.arrow_schema = pa.schema([
            pa.field("DBName", pa.string(), nullable=False),
            pa.field("MetricName", pa.string(), nullable=False),
            pa.field("Data", pa.binary(), nullable=False),
        ])

    def UpdateMeasurements(self, request, context):
        dataRows = [json_format.MessageToDict(msg) for msg in request.Data]
        jsonData = json.dumps(dataRows)

        row = [{
            "DBName": request.DBName,
            "MetricName": request.MetricName,
            "Data": jsonData,
        }]

        df = pa.Table.from_pylist(row, schema=self.arrow_schema)
        self.tbl.append(df)

        return Reply(logmsg="Metrics Inserted in iceberg.")