from dataclasses import dataclass
from queue import Queue
from typing import Any, Optional

from protos import task_queue_pb2, task_queue_pb2_grpc


@dataclass
class TaskQueueServicer(task_queue_pb2_grpc.TaskQueueServicer):
    """This class implements the grcp servicer method used by the driver."""

    num_of_buckets: int
    task_queue: Queue[task_queue_pb2.Task] = Queue()

    def GetTask(
        self, request: task_queue_pb2.Request, context: Any
    ) -> Optional[task_queue_pb2.Task]:
        print(f"Worker {request.worker_id} requesting task")
        if not self.task_queue.empty():
            return self.task_queue.get()
        return None

    def GetNumberOfBuckets(
        self, request: task_queue_pb2.Request, context: Any
    ) -> task_queue_pb2.NumberOfBuckets:
        print(f"Worker {request.worker_id} requesting number of buckets")
        return task_queue_pb2.NumberOfBuckets(
            num_of_buckets=self.num_of_buckets,
        )
