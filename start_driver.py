import sys

from driver.driver import Driver


if len(sys.argv) < 4:
    raise TypeError("Too few arguments")
if len(sys.argv[2]) < len(sys.argv[1]):
    raise TypeError("Number of reduce tasks cannot be less than number of map tasks")

driver: Driver = Driver(
    num_of_map_tasks=sys.argv[1],
    num_of_reduce_tasks=sys.argv[2],
    file_path=sys.argv[3],
)
driver.run()
