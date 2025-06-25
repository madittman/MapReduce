import os
import sys
from dataclasses import dataclass, field
from typing import List


@dataclass
class Driver:
    num_of_map_tasks: int
    num_of_reduce_tasks: int
    file_path: str
    files: List[str] = field(init=False)

    def _collect_files(self) -> None:
        files: List[str] = os.listdir(self.file_path)

    def run(self):
        pass


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise TypeError("Too few arguments")
    if len(sys.argv[2]) < len(sys.argv[1]):
        raise TypeError(
            "Number of reduce tasks cannot be less than number of map tasks"
        )

    driver = Driver(
        num_of_map_tasks=sys.argv[1],
        num_of_reduce_tasks=sys.argv[2],
        file_path=sys.argv[3],
    )
    driver.run()
