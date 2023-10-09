#  Patronus: High-Performance and Protective Remote Memory

Patronus is a high-performance RDMA framework with access protection semantics (enabled in userspace).

For more details, please refer to our [paper](https://www.usenix.org/conference/fast23/presentation/yan):

[FAST'23] Patronus: High-Performance and Protective Remote Memory.

## System Requirements
- Mellanox ConnectX-5 NICs and above (other versions are not tested).
- RDMA Driver: MLNX_OFED_LINUX-4.* (If you use MLNX_OFED_LINUX-5**, you should modify codes to resolve interface incompatibility)
- memcached (to exchange QP information)

## API

See [Patronus.h](include/patronus/Patronus.h).

``` c++
// Access permission (lease) management
// Get read-only lease
Lease get_rlease(uint16_t node_id,
                uint16_t dir_id,
                GlobalAddress bind_gaddr,
                uint64_t alloc_hint,
                size_t size,
                std::chrono::nanoseconds ns,
                flag_t flag /* AcquireRequestFlag */,
                CoroContext *ctx);
// Get read-write lease
Lease get_wlease(uint16_t node_id,
                uint16_t dir_id,
                GlobalAddress bind_gaddr,
                uint64_t alloc_hint,
                size_t size,
                std::chrono::nanoseconds ns,
                flag_t flag /* AcquireRequestFlag */,
                CoroContext *ctx = nullptr);
// Relinquish the lease
void relinquish(Lease &lease,
                uint64_t hint,
                flag_t flag /* LeaseModifyFlag */,
                CoroContext *ctx = nullptr);
// RDMA one-sided access API
inline RetCode read(Lease &lease,
                    char *obuf,
                    size_t size,
                    size_t offset,
                    flag_t flag /* RWFlag */,
                    CoroContext *ctx,
                    TraceView = util::nulltrace);
inline RetCode write(Lease &lease,
                    const char *ibuf,
                    size_t size,
                    size_t offset,
                    flag_t flag /* RWFlag */,
                    CoroContext *ctx,
                    TraceView = util::nulltrace);
inline RetCode cas(Lease &lease,
                    char *iobuf,
                    size_t offset,
                    uint64_t compare,
                    uint64_t swap,
                    flag_t flag /* RWFlag */,
                    CoroContext *ctx,
                    TraceView = util::nulltrace);
inline RetCode faa(Lease &lease,
                    char *iobuf,
                    size_t offset,
                    int64_t value,
                    flag_t flag /* RWFlag */,
                    CoroContext *ctx,
                    TraceView = util::nulltrace);

```

## Prerequisite

### Configure memcached

Deploy a memcached service.

Modify `memcached.conf` to set ip and port of memcached correctly.

### Configure Cluster Membership

Modify `script/inet.conf`, `script/vm.conf` for the lists of nodes in the cluster.

Modify `include/Common.h`:

``` c++
constexpr static size_t kMachineNr = 4; // modify to the actual number of machines
```

for the actual number of nodes.

### Configure your working directory

modify `script/env.sh`, especially modify `REMOTE_DIR` variable for your own working directory.

### Install Dependencies Cluster-wise

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

## Run

``` bash
cd script
./bench.sh <the name of the executable>
# for example, the below codes will run `test/main.cpp` cluster-wide
./bench.sh main
```

After running the codes, the cluster-wise logs are stored in `script/fetched/` directory. You can check the logs.