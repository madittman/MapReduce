import grpc
import os
import re
from dataclasses import dataclass, field
from time import sleep
from typing import Dict, List, Union

from protos import task_queue_pb2, task_queue_pb2_grpc


@dataclass
class Worker:
    pid: int = os.getpid()
    num_of_buckets: Union[int, None] = field(
        default=None, init=False
    )  # set later in run method

    @staticmethod
    def _get_words_by_count(input_files: List[str]) -> Dict[str, int]:
        """Return map of each word by count."""
        words_by_count: Dict[str, int] = {}
        pattern: str = r"[a-zA-Z0-9'-]+"
        for filename in input_files:
            with open(filename, "r") as file:
                content: str = file.read()
                words: List[str] = re.findall(pattern, content)
                for word in words:
                    if word not in words_by_count.keys():
                        words_by_count[word] = 1
                    else:
                        words_by_count[word] += 1
        return words_by_count

    def _process_map_task(self, task_id: int, files: List[str]) -> None:
        """Count words in each file and write <word count> to intermediate files."""
        print(
            f"Worker {self.pid}: Processing map task, task_id = {task_id}, files = {files}\n"
        )
        words_by_count: Dict[str, int] = self._get_words_by_count(files)

        # Divide words_by_count further into buckets
        buckets: Dict[int, Dict[str, int]] = {}
        for _bucket_id in range(self.num_of_buckets):
            buckets[_bucket_id] = {}
        for word, count in words_by_count.items():
            # The first character of each word decides which bucket it goes into
            bucket_id: int = ord(word[0]) % self.num_of_buckets
            buckets[bucket_id][word] = count

        # Create folder for intermediate files if not already done by another worker
        intermediate_path: str = "./intermediate_files"
        if not os.path.exists(intermediate_path):
            print(f"Worker {self.pid}: Creating intermediate folder")
            os.makedirs(intermediate_path)

        # Create and write words by count to intermediate files
        for bucket_id in range(self.num_of_buckets):
            filename: str = os.path.join(intermediate_path, f"mr-{task_id}-{bucket_id}")
            with open(filename, "w") as file:
                for word, count in buckets[bucket_id].items():
                    file.write(f"{word} {count}\n")

    def _process_reduce_task(self, task_id: int, files: List[str]) -> None:
        """Sum up word counts from files and write to output file."""
        print(
            f"Worker {self.pid}: Processing reduce task, task_id = {task_id}, files = {files}\n"
        )

        # Read in all words by count from the intermediate files
        words_by_count: Dict[str, int] = {}
        for filename in files:
            # TODO: This is a quick and dirty fix! (to prevent raise condition)
            # Wait until file is created by another worker
            while not os.path.isfile(filename):
                print(f"Worker {self.pid}: waiting\n")
                sleep(0.5)
                continue
            with open(filename, "r") as file:
                for line in file:
                    word: str
                    _count: str
                    word, _count = line.split()
                    count: int = int(_count)
                    if word not in words_by_count.keys():
                        words_by_count[word] = count
                    else:
                        words_by_count[word] += count
        # Sort dictionary by descending word count order
        words_by_count = dict(
            sorted(words_by_count.items(), key=lambda item: item[1], reverse=True)
        )

        # Create folder for output files if not already done by another worker
        output_path: str = "./output_files"
        if not os.path.exists(output_path):
            print(f"Worker {self.pid}: Creating output folder")
            os.makedirs(output_path)

        # Write word by count to output file.
        # The output file is bound to the reduce task.
        output_file: str = os.path.join(output_path, f"out-{task_id}")
        with open(output_file, "w") as file:
            for word, count in words_by_count.items():
                file.write(f"{word} {count}\n")

    def run(self) -> None:
        with grpc.insecure_channel("localhost:50051") as channel:
            stub: task_queue_pb2_grpc.TaskQueueStub = task_queue_pb2_grpc.TaskQueueStub(
                channel
            )
            task: task_queue_pb2.Task = stub.GetTask(
                task_queue_pb2.Request(worker_id=self.pid)
            )
            self.num_of_buckets: int = stub.GetNumberOfBuckets(
                task_queue_pb2.Request(worker_id=self.pid)
            ).num_of_buckets

        if task.type == "map":
            self._process_map_task(task.task_id, task.files)
        elif task.type == "reduce":
            self._process_reduce_task(task.task_id, task.files)
        else:
            raise NotImplementedError("Unknown task type")
