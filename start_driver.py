import argparse

from driver.driver import Driver


parser = argparse.ArgumentParser(
    prog="Driver", description="Create map and reduce tasks for workers"
)
parser.add_argument("num_of_map_tasks", type=int, help="Number of map tasks")
parser.add_argument("num_of_reduce_tasks", type=int, help="Number of reduce tasks")
parser.add_argument("file_path", type=str, help="File path to look for")
args = parser.parse_args()
if args.num_of_map_tasks < args.num_of_reduce_tasks:
    raise TypeError("Number of map tasks cannot be less than number of reduce tasks")

driver: Driver = Driver(
    num_of_map_tasks=args.num_of_reduce_tasks,
    num_of_reduce_tasks=args.num_of_reduce_tasks,
    file_path=args.file_path,
)
driver.run()
