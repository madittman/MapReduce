import grpc
import os
from concurrent import futures
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, List

from protos import task_queue_pb2, task_queue_pb2_grpc


@dataclass
class TaskQueueServicer(task_queue_pb2_grpc.TaskQueueServicer):
    task_queue: Queue[task_queue_pb2.Task] = Queue()

    def GetTask(self, request: task_queue_pb2.Request, context: Any) -> None:
        worker_id: int = request.worker_id
        print("worker_id", worker_id)  # for testing
        if not self.task_queue.empty():
            return self.task_queue.get()
        return None


@dataclass
class Driver:
    num_of_map_tasks: int
    num_of_reduce_tasks: int
    file_path: str

    server: grpc._server._Server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )
    task_queue_servicer: TaskQueueServicer = field(default_factory=TaskQueueServicer)
    files: List[str] = field(init=False)

    def _collect_files(self) -> None:
        self.files: List[str] = os.listdir(self.file_path)

    def run(self) -> None:
        # Initialize some dummy tasks for testing
        tasks = [
            task_queue_pb2.Task(task_id=1, type="map", files=[]),
            task_queue_pb2.Task(task_id=2, type="map", files=[]),
            task_queue_pb2.Task(task_id=3, type="reduce", files=[]),
            task_queue_pb2.Task(task_id=4, type="map", files=[]),
            task_queue_pb2.Task(task_id=5, type="map", files=[]),
            task_queue_pb2.Task(task_id=6, type="reduce", files=[]),
            task_queue_pb2.Task(task_id=7, type="map", files=[]),
            task_queue_pb2.Task(task_id=8, type="map", files=[]),
            task_queue_pb2.Task(task_id=9, type="reduce", files=[]),
        ]
        for task in tasks:
            self.task_queue_servicer.task_queue.put(task)

        task_queue_pb2_grpc.add_TaskQueueServicer_to_server(
            self.task_queue_servicer, self.server
        )
        self.server.add_insecure_port("[::]:50051")
        self.server.start()
        while not self.task_queue_servicer.task_queue.empty():
            self.server.wait_for_termination(timeout=5)

        print("Driver finished")
        self.server.stop(grace=0)
