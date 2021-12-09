import grpc
from concurrent import futures
import example_pb2
import example_pb2_grpc


class ExampleServicer(example_pb2_grpc.ExampleServicer):
    def Average(self, request_iterator, context):
        for request in request_iterator:
            values = map(
                lambda d: example_pb2.OutputDatum(DateTime=d.DateTime, Value=(d.Value1 + d.Value2) / 2),
                request.Data
            )
            output = example_pb2.Output()
            output.Data.extend(values)
            yield output


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    example_pb2_grpc.add_ExampleServicer_to_server(ExampleServicer(), server)
    server.add_insecure_port('[::]:9900')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
