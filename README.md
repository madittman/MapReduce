# MapReduce
This is a simple implementation of distributed program using [MapReduce](https://en.wikipedia.org/wiki/MapReduce). 
<br>
The whole task can be found [here](map-reduce-task.pdf).

The code is written and tested on **[Python 3.13.5](https://www.python.org/downloads/release/python-3135/)**.

## Quick Start

Build the necessary files and start the driver passing the arguments N, M and F for
<br>
_N = number of map tasks
<br>
M = number of reduce tasks
<br>
F = file path to look for_

For example:
```sh
make
python start_driver.py 6 4 ./test_files
```

In others shells, you can now start an arbitrary number of workers that will all process the tasks split by the driver:
```sh
python start_worker.py
```
