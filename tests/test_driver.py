import os
from typing import List

from driver import driver
from protos import task_queue_pb2


FILEPATH: str = os.path.abspath("./tests/data/")
INPUT_FILES: List[str] = [
    "pg-tom_sawyer.txt",
    "pg-frankenstein.txt",
    "pg-sherlock_holmes.txt",
    "pg-huckleberry_finn.txt",
    "pg-metamorphosis.txt",
    "pg-dorian_gray.txt",
    "pg-being_ernest.txt",
    "pg-grimm.txt",
]
INPUT_FILES = [os.path.join(FILEPATH, input_file) for input_file in INPUT_FILES]


def test_create_tasks_1_1():
    """1 Map Task, 1 Reduce Task"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=1,
        num_of_reduce_tasks=1,
        filepath=FILEPATH,
    )

    # Assert map tasks
    map_tasks: List[task_queue_pb2.Task] = test_driver._get_map_tasks()
    assert len(map_tasks) == 1
    assert map_tasks[0] == task_queue_pb2.Task(
        task_id=0,
        type="map",
        files=INPUT_FILES,
    )

    # Assert reduce tasks
    reduce_tasks: List[task_queue_pb2.Task] = test_driver._get_reduce_tasks()
    assert len(reduce_tasks) == 1
    assert reduce_tasks[0] == task_queue_pb2.Task(
        task_id=1,
        type="reduce",
        files=["mr-0-0"],
    )


def test_create_tasks_3_2():
    """3 Map Tasks, 2 Reduce Tasks"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=3,
        num_of_reduce_tasks=2,
        filepath=FILEPATH,
    )

    # Assert map tasks
    map_tasks: List[task_queue_pb2.Task] = test_driver._get_map_tasks()
    assert len(map_tasks) == 3
    assert map_tasks[0] == task_queue_pb2.Task(
        task_id=0,
        type="map",
        files=INPUT_FILES[0:3],
    )
    assert map_tasks[1] == task_queue_pb2.Task(
        task_id=1,
        type="map",
        files=INPUT_FILES[3:6],
    )
    assert map_tasks[2] == task_queue_pb2.Task(
        task_id=2,
        type="map",
        files=INPUT_FILES[6:8],
    )

    # Assert reduce tasks
    reduce_tasks: List[task_queue_pb2.Task] = test_driver._get_reduce_tasks()
    assert len(reduce_tasks) == 2
    assert reduce_tasks[0] == task_queue_pb2.Task(
        task_id=3,
        type="reduce",
        files=["mr-0-0", "mr-0-1", "mr-1-0", "mr-1-1"],
    )
    assert reduce_tasks[1] == task_queue_pb2.Task(
        task_id=4,
        type="reduce",
        files=["mr-2-0", "mr-2-1"],
    )


def test_create_tasks_9_3():
    """9 Map Tasks, 3 Reduce Tasks"""
    test_driver: driver.Driver = driver.Driver(
        num_of_map_tasks=9,
        num_of_reduce_tasks=3,
        filepath=FILEPATH,
    )

    # Assert map tasks
    map_tasks: List[task_queue_pb2.Task] = test_driver._get_map_tasks()
    assert len(map_tasks) == 9

    # Assert map tasks
    for index, file in enumerate(INPUT_FILES):
        assert map_tasks[index] == task_queue_pb2.Task(
            task_id=index,
            type="map",
            files=[file],
        )
    # Last map task has no files
    assert map_tasks[8] == task_queue_pb2.Task(
        task_id=8,
        type="map",
        files=[],
    )

    # Assert reduce tasks
    reduce_tasks: List[task_queue_pb2.Task] = test_driver._get_reduce_tasks()
    assert len(reduce_tasks) == 3
    assert reduce_tasks[0] == task_queue_pb2.Task(
        task_id=9,
        type="reduce",
        files=[
            "mr-0-0",
            "mr-0-1",
            "mr-0-2",
            "mr-1-0",
            "mr-1-1",
            "mr-1-2",
            "mr-2-0",
            "mr-2-1",
            "mr-2-2",
        ],
    )
    assert reduce_tasks[1] == task_queue_pb2.Task(
        task_id=10,
        type="reduce",
        files=[
            "mr-3-0",
            "mr-3-1",
            "mr-3-2",
            "mr-4-0",
            "mr-4-1",
            "mr-4-2",
            "mr-5-0",
            "mr-5-1",
            "mr-5-2",
        ],
    )
    assert reduce_tasks[2] == task_queue_pb2.Task(
        task_id=11,
        type="reduce",
        files=[
            "mr-6-0",
            "mr-6-1",
            "mr-6-2",
            "mr-7-0",
            "mr-7-1",
            "mr-7-2",
            "mr-8-0",
            "mr-8-1",
            "mr-8-2",
        ],
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
