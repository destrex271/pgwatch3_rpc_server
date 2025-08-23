from pgwatch_pb2_grpc import ReceiverServicer
from pgwatch_pb2 import Reply

class Receiver(ReceiverServicer):
    def UpdateMeasurements(self, request, context):
        return super().UpdateMeasurements(request, context)