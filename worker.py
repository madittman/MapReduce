from dataclasses import dataclass


@dataclass
class Worker:
    def run(self):
        pass


if __name__ == "__main__":
    worker = Worker()
    worker.run()
