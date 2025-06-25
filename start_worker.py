import grpc

from worker.worker import Worker

worker: Worker = Worker()
driver_finished: bool = False
while not driver_finished:
    try:
        worker.run()
    except grpc._channel._InactiveRpcError:
        print("Driver finished")
        driver_finished = True
