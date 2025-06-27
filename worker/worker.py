import grpc
import os
from dataclasses import dataclass
from time import sleep

from protos import task_queue_pb2, task_queue_pb2_grpc


@dataclass
class Worker:
    pid: int = os.getpid()

    def run(self) -> None:
        with grpc.insecure_channel("localhost:50051") as channel:
            stub: task_queue_pb2_grpc.TaskQueueStub = task_queue_pb2_grpc.TaskQueueStub(
                channel
            )
            task: task_queue_pb2.Task = stub.GetTask(
                task_queue_pb2.Request(worker_id=self.pid)
            )
        if task.type == "map":
            self._process_map_task(task.task_id, task.files)
        elif task.type == "reduce":
            self._process_reduce_task(task.task_id, task.files)
        else:
            raise NotImplementedError("Unknown task type")

    def _process_map_task(self, task_id: int, files: str) -> None:
        print("Processing map task, task_id = {}, files = {}".format(task_id, files))
        sleep(2)  # for testing

    def _process_reduce_task(self, task_id: int, files: str) -> None:
        print("Processing reduce task, task_id = {}, files = {}".format(task_id, files))
        sleep(2)  # for testing
