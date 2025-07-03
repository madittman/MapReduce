# MapReduce
A simple implementation of the [MapReduce](https://en.wikipedia.org/wiki/MapReduce) model. 
<br>
The whole task can be found [here](map-reduce-task.pdf).

The code is written and tested in [Python 3.13.5](https://www.python.org/downloads/release/python-3135/).

## Quick Start

Build the necessary files and start the driver passing the arguments **m**, **r** and **f** for
<br>
**m = number of map tasks
<br>
r = number of reduce tasks
<br>
f = filepath to look for**

For example:
```sh
make
./driver.sh 6 4 tests/data/
```
This starts the driver creating **6 map tasks** and **4 reduce tasks** based on all files in the folder **tests/data/**.
<br>
For simplicity, the input files are not split.
<br>
If there are fewer input files than map tasks, the driver creates some map tasks with an empty file list.
<br>
The number of map tasks cannot be less than the number of reduce tasks.

In another shell, you can now start an arbitrary number of workers that will all fetch a task one by one and process it.
<br>
For example, this will start 3 different worker processes:
```sh
./worker.sh 3
```
It works best when there are many input files and only several workers.
<br>
The workers will always create **MxN** intermediate files and **N** output files (**M** = number of map tasks,
<br>
**N** = number of reduce tasks). If there are not enough input files the remaining files will be empty.

## Run Tests

Run the tests for the driver by calling **pytest**:
```sh
pytest tests/test_driver.py
```

## Clean Project

To start a clean project from scratch, run
```sh
make clean
```
This deletes all compiled proto files, pycaches and the file folders that are created by the workers.