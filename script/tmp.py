#!/usr/bin/python3
import subprocess
import sys
import config
import cluster
import util
from cluster import Cluster
import pprint
import os
import pm


# if __name__ == '__main__':
#     if len(sys.argv) <= 1:
#         print("{} bench_file [flags]".format(sys.argv[0]))
#         exit(-1)
#     binary = sys.argv[1]
#     params = []
#     if len(sys.argv) >= 2:
#         params = sys.argv[2:]

#     c = cluster.Cluster(config.NODES)

#     prepare(c)

#     c.benchmark(binary, params, hooks=[
#                 'valgrind', '--leak-check=yes', '--leak-check=full'])
#     c.benchmark(binary, params, hooks=[
#                 'pcm-pcie', '0.1', '-e', '-B', '-silent', f'-csv=pcie.{binary}.{util.generate_exec_meta(0)}.csv'])
#     c.benchmark(binary, params, hooks=[
#                 'perf', 'record', '-a', '-g', '--'])

#     fetch(c)


# if __name__ == '__main__':
#     c = cluster.Cluster(config.NODES)
#     c.correct_test_all()

def fetch_log_tails(c, node, tail_nr=10):
    log_files = os.path.join(config.BASE_DIR, "*.LOG")
    cmd = ["tail", "-n", f"{tail_nr}", log_files]
    pprint.pprint(cmd)
    processes = c.node_execute_short_cmd_async(node, cmd)
    results = Cluster.get_process_result(processes)
    return results


def handler(line):
    print(f"handler: {line}", end='')


if __name__ == '__main__':

    # c = cluster.Cluster(config.NODES)
    # c.cluster_execute_cmd(sys.argv[1:])
    # c.prepare_benchmark()
    # c.post_benchmark()
    # r = fetch_log_tails(c, '10.0.2.132')
    # Cluster.show_process_result(r)
    # print(util.state("hello world"))
    # with open("../memcached.conf") as f:
    #     s = f.read().strip().split()
    #     print(s)
    # print(util.query_cmake("CMAKE_BUILD_TYPE"))
    # print(util.query_cmake("CMAKE_BUILD_TYPE"))
    # print(util.query_asan_enabled())
    # util.report_compile_info()
    # p = pm.launch_local_process("test")
    # p.run("pwd")
    # p.wait(silent=False)

    # p = pm.launch_remote_process("test_remote", "10.0.2.132")
    # p.run("pwd")
    # p.wait(silent=False)

    # p = pm.launch_remote_process_inline("test2", "10.0.2.132", "pwd")
    # p.wait(silent=False)
    # p = pm.launch_remote_process_inline(
    #     "test2", "10.0.2.132", "'coredumpctl -1'", False)
    # for line in p.stdout():
    #     print(f"!!! {line}", end='')
    # p.wait()

    # p = pm.launch_remote_process_inline(
    #     "test", "10.0.2.132", ["coredumpctl",  "-1"], False)
    # p.wait()
    # p = pm.launch_local_process_inline
    util.report_compile_info()
