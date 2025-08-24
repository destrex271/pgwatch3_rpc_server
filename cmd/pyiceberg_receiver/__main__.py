import grpc
import argparse
from concurrent import futures
from iceberg_receiver import Receiver
from pgwatch_pb2_grpc import add_ReceiverServicer_to_server

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

def serve(port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ReceiverServicer_to_server(
        Receiver(args.icebergDataDir),
        server,
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"gRPC server started, listening on port {port}")
    server.wait_for_termination()

serve(args.port)