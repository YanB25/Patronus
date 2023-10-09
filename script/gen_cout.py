#!/usr/bin/python3
import sys
import pm
if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print(f"{sys.argv[0]} filename")
        exit(-1)

    file = sys.argv[1]
    p = pm.launch_local_process_inline(
        "gen_cout", ["../tools/build/generate", "-p",  "../build/", file], False)
    p.wait()
