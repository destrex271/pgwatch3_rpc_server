import grpc
from concurrent import futures
from iceberg_receiver import Receiver
from pgwatch_pb2_grpc import add_ReceiverServicer_to_server

def serve(port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ReceiverServicer_to_server(
        Receiver(),
        server,
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve(1234)