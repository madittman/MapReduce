import argparse
import os

from driver.driver import Driver


parser: argparse.ArgumentParser = argparse.ArgumentParser(
    prog="Driver", description="Create map and reduce tasks for workers"
)
parser.add_argument("m", type=int, help="Number of map tasks")
parser.add_argument("r", type=int, help="Number of reduce tasks")
parser.add_argument("f", type=str, help="Filepath to look for")
args: argparse.Namespace = parser.parse_args()

driver: Driver = Driver(
    num_of_map_tasks=args.m,
    num_of_reduce_tasks=args.r,
    filepath=os.path.abspath(args.f),  # convert to absolute path
)
if driver.error is not None:
    print("Driver finished with error")
    exit(1)
driver.run()
