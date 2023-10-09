#  Sherman
## Prerequisite

### Configure memcached

modify `memcached.conf` to set ip and port of memcached correctly.

### Configure the members of the cluster
modify `script/inet.conf`, `script/vm.conf` for the lists of nodes in the cluster.

modify `include/Common.h`:

``` c++
constexpr static size_t kMachineNr = 4; // modify to the actual number of machines
```

for the actual number of nodes.

### Configure your working directory
modify `script/env.sh`, especially modify `REMOTE_DIR` variable for your own working directory.

### Install Dependencies

Then, run the following codes to setup the environment.

``` bash
sudo ./bootstrap.sh
cd script
./for_each.sh ./bootstrap.sh
```

## Build the project

``` bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..; make -j
# or in Debug mode
# cmake -DCMAKE_BUILD_TYPE=Debug ..; make -j
```

You can also use helper scripts

``` bash
cd script
./build_release.sh
# or in Debug mode
# ./build_debug.sh
```

## How to run

``` bash
cd script
./bench.sh <the name of the executable>
# for example, the below codes will run `test/main.cpp` cluster-wide
./bench.sh main
```

After running the codes, the logs will be fetched, and located at `script/fetched/`. You can check the logs.

See `test/main.sh` and `test/atomic_latency.cpp` to understand how to use DSM.

See `include/DSM.h`, `src/DSM.cpp`, `include/Rdma.h`, `src/rdma/Operation.cpp`, `src/Directory.cpp` and `src/ThreadConnection.cpp` for detailed implementation of DSM.

## How to debug

If your program crashed, you can checks the coredump by

``` bash
# in one of the node
# check the lastest core dump
sudo coredump -1
# automatically boot gdb into the core dump
sudo coredump gdb
```
