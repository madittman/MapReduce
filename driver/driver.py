import grpc
import os
import pprint
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
    tasks: List[task_queue_pb2.Task] = field(default_factory=list)

    def _collect_files(self) -> None:
        self.files: List[str] = os.listdir(self.file_path)

    def _create_tasks(self) -> None:
        map_tasks: List[task_queue_pb2.Task] = []
        reduce_tasks: List[task_queue_pb2.Task] = []

        files_by_map_task: List[List[str]] = [[] for _ in range(self.num_of_map_tasks)]
        for index, filename in enumerate(self.files):
            task_id = (
                index % self.num_of_map_tasks
            )  # keep task id in range(0, num_of_map_tasks)
            files_by_map_task[task_id].append(
                filename
            )  # filenames get assigned to map tasks sequentially

        for task_id in range(self.num_of_map_tasks):
            map_task: task_queue_pb2.Task = task_queue_pb2.Task(
                task_id=task_id,
                type="map",
                files=files_by_map_task[task_id],
            )
            map_tasks.append(map_task)

        # TODO: Implement creating reduce tasks

        self.tasks.extend(map_tasks)
        self.tasks.extend(reduce_tasks)
        pprint.pp(map_tasks)  # for testing

    def run(self) -> None:
        self._collect_files()
        self._create_tasks()
        for task in self.tasks:
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
