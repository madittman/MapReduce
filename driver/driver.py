import grpc
import math
import os
import pprint
from concurrent import futures
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, List, Union

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
        # all files from the same map task


@dataclass
class Driver:
    num_of_map_tasks: int
    num_of_reduce_tasks: int
    filepath: str

    server: grpc._server._Server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )
    task_queue_servicer: TaskQueueServicer = field(default_factory=TaskQueueServicer)
    input_files: List[str] = field(init=False)  # set in __post_init__ method
    intermediate_files: List[str] = field(init=False)  # set in __post_init__ method
    tasks: List[task_queue_pb2.Task] = field(
        init=False, default_factory=list
    )  # all map and reduce tasks
    error: Union[Exception, None] = field(
        init=False, default=None
    )  # set when error occurs in __post_init__

    # all files from the same map task
    def __post_init__(self):
        try:
            if self.num_of_map_tasks < 1 or self.num_of_reduce_tasks < 1:
                raise TypeError("Number of map and reduce tasks cannot be less than 1")
            if self.num_of_map_tasks < self.num_of_reduce_tasks:
                raise TypeError(
                    "Number of map tasks cannot be less than number of reduce tasks"
                )
            # Get list of input files
            self.input_files = os.listdir(self.filepath)

            # Create list of intermediate files
            # Intermediate files have the format "mr-<map_task_id>-<bucket_id>"
            # (bucket ids are the ids for the reduce tasks)
            self.intermediate_files = []
            for map_task_id in range(self.num_of_map_tasks):
                files_by_map_task_id: List[str] = [
                    f"mr-{map_task_id}-{bucket_id}"
                    for bucket_id in range(self.num_of_reduce_tasks)
                ]
                self.intermediate_files.extend(files_by_map_task_id)

        except TypeError as error:
            self.error = error
            print("TypeError:", self.error)
        except FileNotFoundError as error:
            self.error = error
            print("FileNotFoundError:", self.error)
        except Exception as error:
            self.error = error
            print("Unknown Exception", error)
            raise

    @staticmethod
    def _distribute_files_to_tasks(
        files: List[List[str]], num_of_tasks: int, max_num_of_files: int
    ) -> List[List[str]]:
        """
        Distribute files among tasks.
        max_num_of_files is the maximum number of files for each task.
        """
        files_by_task: List[List[str]] = [[] for _ in range(num_of_tasks)]
        start_index: int = 0
        for idx in range(num_of_tasks):
            files_by_task[idx].extend(
                files[start_index : start_index + max_num_of_files]
            )
            start_index += max_num_of_files
        return files_by_task

    def _get_files_by_map_task(self) -> List[List[str]]:
        """Return list of files by map task id."""
        max_num_of_files: int = math.ceil(len(self.input_files) / self.num_of_map_tasks)
        return self._distribute_files_to_tasks(
            self.input_files, self.num_of_map_tasks, max_num_of_files
        )

    def _get_files_by_bucket(self) -> List[List[str]]:
        """Return list of files by reduce task (bucket)."""
        # A file packet are all files from the same map task, e.g. ["mr-0-1", "mr-0-2", "mr-0-3"]
        max_num_of_file_packets = math.ceil(
            self.num_of_map_tasks / self.num_of_reduce_tasks
        )
        max_num_of_files = max_num_of_file_packets * self.num_of_reduce_tasks
        return self._distribute_files_to_tasks(
            self.intermediate_files, self.num_of_reduce_tasks, max_num_of_files
        )

    def _create_map_tasks(self) -> None:
        """Create map and tasks based on list of input files."""
        map_tasks: List[task_queue_pb2.Task] = []
        files_by_map_task: List[List[str]] = self._get_files_by_map_task()
        for map_task_id in range(self.num_of_map_tasks):
            map_task: task_queue_pb2.Task = task_queue_pb2.Task(
                task_id=map_task_id,
                type="map",
                files=files_by_map_task[map_task_id],
            )
            map_tasks.append(map_task)
        self.tasks.extend(map_tasks)

    def _create_reduce_tasks(self) -> None:
        """Create reduce tasks based on list of intermediate files."""
        reduce_tasks: List[task_queue_pb2.Task] = []
        files_by_bucket: List[List[str]] = self._get_files_by_bucket()
        for bucket_id in range(self.num_of_reduce_tasks):
            reduce_task: task_queue_pb2.Task = task_queue_pb2.Task(
                task_id=bucket_id + self.num_of_map_tasks,  # task ids are incremented
                type="reduce",
                files=files_by_bucket[bucket_id],
            )
            reduce_tasks.append(reduce_task)
        self.tasks.extend(reduce_tasks)

    def run(self) -> None:
        """Create map and reduce tasks and put them into the task queue."""
        self._create_map_tasks()
        self._create_reduce_tasks()

        pprint.pp(self.tasks)  # for testing

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
