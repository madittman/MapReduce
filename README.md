# MapReduce
This is a simple implementation of distributed program using [MapReduce](https://en.wikipedia.org/wiki/MapReduce). 
<br>
The whole task can be found [here](map-reduce-task.pdf).

The code is written and tested in [Python 3.13.5](https://www.python.org/downloads/release/python-3135/).

## Quick Start

Build the necessary files and start the driver passing the arguments _m_, _r_ and _f_ for
<br>
_m = number of map tasks
<br>
r = number of reduce tasks
<br>
f = filepath to look for_

For example:
```sh
make
./start_driver.sh 6 4 tests/data/
```
This starts the driver creating _6 map tasks_ and _4 reduce tasks_ based on all files in the folder _tests/data/_.

In another shell, you can now start an arbitrary number of workers that will all fetch a task one by one and process it.
<br>
For example, this will start 3 different worker processes:
```sh
./start_worker.sh 3
```

## Run Tests

Run the tests by calling _pytest_:
```sh
pytest tests/test_driver.py
```