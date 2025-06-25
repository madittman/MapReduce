import grpc
import os
from dataclasses import dataclass
from time import sleep

from protos import task_queue_pb2, task_queue_pb2_grpc


@dataclass
class Worker:
    pid: int = os.getpid()

    def run(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = task_queue_pb2_grpc.TaskQueueStub(channel)
            response = stub.GetTask(task_queue_pb2.Request(worker_id=self.pid))
        sleep(2)  # for testing
        print(f"Response: {response}")
