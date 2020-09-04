#  Sherman
## How to build
TODO:

## How to run

``` bash
/usr/local/openmpi/bin/mpirun --allow-run-as-root -hostfile host -np 8 ./atomic_inbound 8 20 0.99 0 8
```

where host is 

```
10.0.2.113 slots=1
10.0.2.114 slots=1
10.0.2.115 slots=1
10.0.2.116 slots=1
10.0.2.117 slots=1
10.0.2.118 slots=1
10.0.2.119 slots=1
10.0.2.121 slots=1
```

run `./restartMemc.sh` between each run.