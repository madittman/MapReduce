import os
from typing import List

from driver import driver
from protos import task_queue_pb2


FILEPATH: str = os.path.abspath("./tests/data/")
FILES: List[str] = [
    "pg-tom_sawyer.txt",
    "pg-frankenstein.txt",
    "pg-sherlock_holmes.txt",
    "pg-huckleberry_finn.txt",
    "pg-metamorphosis.txt",
    "pg-dorian_gray.txt",
    "pg-being_ernest.txt",
    "pg-grimm.txt",
]


def test_create_tasks_1_1():
    """1 Map Task, 1 Reduce Task"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=1,
        num_of_reduce_tasks=1,
        filepath=FILEPATH,
    )
    test_driver._create_tasks()

    tasks: List[task_queue_pb2.Task] = test_driver.tasks
    assert len(tasks) == 1
    assert tasks[0] == task_queue_pb2.Task(
        task_id=0,
        type="map",
        files=FILES,
    )


def test_create_tasks_3_2():
    """3 Map Tasks, 2 Reduce Tasks"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=3,
        num_of_reduce_tasks=2,
        filepath=FILEPATH,
    )
    test_driver._create_tasks()

    tasks: List[task_queue_pb2.Task] = test_driver.tasks
    assert len(tasks) == 3
    assert tasks[0] == task_queue_pb2.Task(
        task_id=0,
        type="map",
        files=[FILES[0], FILES[3], FILES[6]],
    )
    assert tasks[1] == task_queue_pb2.Task(
        task_id=1,
        type="map",
        files=[FILES[1], FILES[4], FILES[7]],
    )
    assert tasks[2] == task_queue_pb2.Task(
        task_id=2,
        type="map",
        files=[FILES[2], FILES[5]],
    )


def test_create_tasks_9_3():
    """9 Map Tasks, 3 Reduce Tasks"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=9,
        num_of_reduce_tasks=3,
        filepath=FILEPATH,
    )
    test_driver._create_tasks()

    tasks: List[task_queue_pb2.Task] = test_driver.tasks
    assert len(tasks) == 9
    for index, file in enumerate(FILES):
        assert tasks[index] == task_queue_pb2.Task(
            task_id=index,
            type="map",
            files=[file],
        )
    # Last map task has no files
    assert tasks[8] == task_queue_pb2.Task(
        task_id=8,
        type="map",
        files=[],
    )


def test_type_error():
    """3 Map Tasks, 4 Reduce Tasks (Throws exception)"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=3,
        num_of_reduce_tasks=4,
        filepath=FILEPATH,
    )
    assert isinstance(test_driver.error, TypeError)
    assert (
        str(test_driver.error)
        == "Number of map tasks cannot be less than number of reduce tasks"
    )


def test_file_not_found_error():
    """Set invalid filepath"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=3,
        num_of_reduce_tasks=3,
        filepath="./invalid_path",
    )
    assert isinstance(test_driver.error, FileNotFoundError)
    assert (
        str(test_driver.error)
        == "[Errno 2] No such file or directory: './invalid_path'"
    )
