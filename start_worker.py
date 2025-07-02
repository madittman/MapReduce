import grpc

from worker.worker import Worker


worker: Worker = Worker()
driver_finished: bool = False
while not driver_finished:
    try:
        worker.run()
    except grpc._channel._InactiveRpcError:
        print(f"Worker {worker.pid}: Driver finished\n")
        driver_finished = True
