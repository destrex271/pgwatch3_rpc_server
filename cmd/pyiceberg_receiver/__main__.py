import grpc
import argparse
from concurrent import futures
from iceberg_receiver import Receiver
from pgwatch_pb2_grpc import add_ReceiverServicer_to_server
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    StringType,
    NestedField, 
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-p", "--port", 
    type=int,
    dest="port", 
    required=True, 
    action="store", 
    help="The port number to use for the gRPC server."
)

parser.add_argument(
    "-d", "--iceberg-data-dir",
    type=str, 
    dest="icebergDataDir",
    metavar="DIR",
    required=True,
    action="store",
    help="Directory to store iceberg tables in."
)
args = parser.parse_args()

catalog = load_catalog("pgcatalog")
catalog.create_namespace_if_not_exists("pgwatch")

schema = Schema(
    NestedField(field_id=1, name="DBName", field_type=StringType(), required=True),
    NestedField(field_id=2, name="MetricName", field_type=StringType(), required=True),
    NestedField(field_id=4, name="Data", field_type=StringType(), required=True),
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
    location=args.icebergDataDir,
    partition_spec=partition_spec
)

def serve(port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ReceiverServicer_to_server(
        Receiver(tbl),
        server,
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"gRPC server started, listening on port {port}")
    server.wait_for_termination()

serve(args.port)