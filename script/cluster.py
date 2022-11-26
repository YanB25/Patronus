#!/usr/python3
import os
import pprint
import config
import pathlib
import util
import signal
import pm


class Cluster:
    def __init__(self, nodes):
        self.__nodes = nodes
        # filter out the same node with different numa
        self.__unique_nodes = {node for (node, _) in self.__nodes}
        self.__wait_process = []
        pprint.pprint(self.__unique_nodes)

    def nodes(self):
        return self.__nodes

    def unique_nodes(self):
        return self.__unique_nodes

    def cluster_mkdir_async(self, dirname: 'str', silent: 'bool'):
        cmds = ['mkdir', '-p', dirname]
        self.cluster_execute_short_cmd_async(cmds, silent)

    def cluster_mkdir(self, dirname: 'str', silent: 'bool'):
        self.cluster_mkdir_async(dirname, silent)
        self.__join()

    def kill_all_processes_async(self, silent: 'bool'):
        util.state("kill all processes...")
        files = os.listdir(config.BUILD_DIR)
        # killall does not support more than 32
        silent_msg = ''
        # if silent:
        #     silent_msg = '2>/dev/null'
        for file_chunk in util.chunk(files, 32):
            cmd = ['killall', '--signal SIGKILL', silent_msg, *file_chunk]
            self.cluster_execute_cmd_async(cmd, silent)

    def kill_all_processes(self, silent=True):
        self.kill_all_processes_async(silent)
        self.__join()

    def fetch_results_async(self, silent: 'bool'):
        util.state("fetching results...")
        pathlib.Path(config.FETCH_DIR).mkdir(parents=True, exist_ok=True)
        for ip in self.__unique_nodes:
            remote_dir = config.RESULT_DIR
            local_dir = os.path.join(config.FETCH_DIR, ip)
            self.fetch_dir_async(ip, remote_dir, local_dir, silent)

    def fetch_results(self, silent: 'bool'):
        self.fetch_results_async(silent)
        self.__join()

    def fetch_logs_async(self, silent: 'bool'):
        util.state("fetching logs...")
        pathlib.Path(config.FETCH_DIR).mkdir(parents=True, exist_ok=True)
        for (ip, numa_id) in self.__nodes:
            remote_log = os.path.join(config.BASE_DIR, f"{numa_id}.LOG")
            local_file = os.path.join(config.FETCH_DIR, f"{ip}.{numa_id}.log")
            self.fetch_file_async(ip, remote_log, local_file, silent)

    def fetch_logs(self, silent: 'bool'):
        self.fetch_logs_async(silent)
        self.__join()

    def deploy_sync_async(self, silent: 'bool'):
        util.state("deploy sync...")
        self.cluster_mkdir_async(config.ARTIFACTS_DIR, silent)
        self.cluster_mkdir_async(config.BIN_DIR, silent)
        self.__join()

        for node in self.__unique_nodes:
            # 1) dispatch executables in build/
            all_executables = util.listdir(config.BUILD_DIR, full=True)
            self.dispatch_files_async(
                node, all_executables, config.BIN_DIR, silent)

            # 2) those to BIN_DIR
            to_bin_dirs = ["./report.py", "./coredump.py", "./local_report.sh", "./local_run.py", "./util.py",
                           "./gen_flame_graph.sh", "./parce_pcm.py", "./config.py", "./cluster.py", "./pm.py"]
            # make them full paths
            to_bin_dirs = [os.path.join(config.CUR_DIR, file)
                           for file in to_bin_dirs]
            self.dispatch_files_async(
                node, to_bin_dirs, config.BIN_DIR, silent)

            # 3) those to WORK_DIR
            to_work_dirs = ["../memcached.conf"]
            to_work_dirs = [os.path.join(config.CUR_DIR, file)
                            for file in to_work_dirs]
            self.dispatch_files_async(
                node, to_work_dirs, config.BASE_DIR, silent)

            # 4) to artifacts dir
            self_artifacts_dir = os.path.join(config.CUR_DIR, "../artifacts")
            all_artifacts = util.listdir(self_artifacts_dir, full=True)
            self.dispatch_files_async(
                node, all_artifacts, config.ARTIFACTS_DIR, silent)

    def deploy_sync(self, silent: 'bool'):
        self.deploy_sync_async(silent)
        self.__join()

    def fetch_file_async(self, ip, remote_file: 'str', local_file: 'str', silent):
        '''
        @ip string, the ip address used to ssh
        @remote_file string, the path of remote file
        @local_file string
        '''
        cmd = ['rsync', f"root@{ip}:{remote_file}", local_file]
        self.execute_cmd_async(cmd, ip, silent)

    def dispatch_files_async(self, ip, local_files: 'list[str]', remote_dir: 'str', silent: 'bool'):
        '''
        @ip string
        @local_files [string]
        @remote_dir string, the directory on remote side
        '''
        cmd = ['rsync', *local_files, f"root@{ip}:{remote_dir}"]
        self.execute_cmd_async(cmd, ip, silent)

    def fetch_dir_async(self, ip, remote_dir: 'str', local_dir: 'str', silent: 'bool'):
        '''
        @ip string
        @remote_dir string
        @local_dir string
        '''
        cmd = ['rsync', '-r', f"root@{ip}:{remote_dir}", local_dir]
        self.execute_cmd_async(cmd, ip, silent)

    def execute_cmd_async(self, cmds: 'list[str]', name: 'str', silent: 'bool'):
        '''
        @cmd [list]
        @name string, just a human-readable name
        '''
        p = pm.launch_local_process_inline(name, cmds, silent)
        p.commit()
        self.__wait_process.append(p)

    def execute_cmd(self, cmd: 'list[str]', name: 'str', silent: 'bool'):
        self.execute_cmd_async(cmd, name, silent)
        self.__join()

    def cluster_execute_cmd_async(self, cmds: 'list[str]', silent):
        for node in self.__unique_nodes:
            cmd = ' '.join(cmds)
            name = cmd
            p = pm.launch_remote_process(name, node, silent)
            p.run(cmd)
            p.commit()
            self.__wait_process.append(p)

    def cluster_execute_cmd(self, cmds: 'list[str]', silent: 'bool'):
        self.cluster_execute_cmd_async(cmds, silent)
        self.__join()

    def node_execute_short_cmd(self, node, cmds: 'list[str]', silent: 'bool'):
        self.node_execute_short_cmd_async(node, cmds, silent)
        self.__join()

    def node_execute_short_cmd_async(self, node, cmds: 'list[str]', silent):
        cmd = ' '.join(cmds)
        name = cmd
        p = pm.launch_remote_process_inline(name, node, cmds, silent)
        p.commit()
        self.__wait_process.append(p)
        return p

    def cluster_execute_short_cmd_async(self, cmds: 'list[str]', silent):
        processes = []
        for node in self.__unique_nodes:
            p = self.node_execute_short_cmd_async(node, cmds, silent)
            processes.append(p)
        return processes

    def cluster_execute_short_cmd(self, cmds: 'list[str]', silent):
        self.cluster_execute_short_cmd_async(cmds, silent)
        self.__join()

    def local_test(self, binary: 'str', params: 'list[str]', silent):
        '''
        @binary string, the executable. E.g., 'main'
        @params [string], a list of parameters. E.g., ['--v=3', '--flag=on']
        '''
        util.build_debug(False)
        self.prepare_benchmark(silent)
        self.local_run(binary, params)

    def local_benchmark(self, binary: 'str', params: 'list[str]', hooks, silent):
        self.prepare_benchmark(silent)

        self.__register_post_bench_callback()
        self.__execute_local_benchmark(binary, params, hooks)

        self.post_benchmark(silent)

    @util.report_time_decorator
    @util.report_compile_info_decorator
    def __execute_local_benchmark(self, binary: 'str', params: 'list[str]', hooks=[]):
        '''
        This function has the SAME functionality as __execute_benchmark,
        except that it uses one node.
        @binary string
        @params [string]
        @hooks [string]
        '''

        ip = 'localhost'
        p = pm.launch_remote_process(binary, ip, False)
        p.run(f"cd {config.BIN_DIR}")
        # correctly set environments...
        p.run("source /etc/profile")
        p.run("export TERM='linux'")
        p.run("export GLOG_logtostderr=1")
        p.run("export GLOG_colorlogtostderr=1")
        p.run("export ASAN_OPTIONS=halt_on_error=0")

        # special configs for local benchmarks
        numa_id = 0
        node_id = 0
        machine_nr = 1
        exec_meta = util.generate_exec_meta(numa_id)
        log_path = os.path.join(config.BASE_DIR, f"{numa_id}.LOG")
        rnic = config.select_rnic(numa_id)

        executable = os.path.join(config.BIN_DIR, binary)
        pass_params = ' '.join(params)
        if len(hooks) == 0:
            hooks = ['unbuffer']
        hook_command = ' '.join(hooks)

        redirect_log = f"2>&1 |& tee {log_path}"

        script = f"{hook_command} {executable} {pass_params} --exec_meta='{exec_meta}' {redirect_log}"

        util.state(f"RUNNING {ip}({numa_id}): {script}")
        p.run(script)

        p.commit()
        p.wait()

    @util.report_time_decorator
    def local_run(self, binary: 'str', params: 'list[str]', hooks=[]):
        '''
        This function is handy: it not report experimental results and does not generate logs.
        So, try to make its dependency minimal.
        @binary string
        @params [string]
        @hooks [string], a hook of `executable prefix` that you may want to attach.
        E.g., hooks == ['valgrind', '--leak-check=full'] enables a hook of running valgrind
        E.g., hooks == ['gdb'] enables runs with debugging
        '''
        silent = False

        p = pm.launch_local_process(binary, silent)
        p.run(f"cd {config.BIN_DIR}")
        # correctly set environments...
        p.run("source /etc/profile")
        p.run("export TERM='linux'")
        p.run("export GLOG_logtostderr=1")
        p.run("export GLOG_colorlogtostderr=1")
        p.run("export ASAN_OPTIONS=halt_on_error=0")

        hook_command = ' '.join(hooks)
        executable = os.path.join(config.BIN_DIR, binary)
        pass_params = ' '.join(params)
        executable_params = '--no_csv=true'

        script = f"{hook_command} {executable} {pass_params} {executable_params}"
        util.state(f"RUNNING {script}")
        p.run(script)

        p.commit()
        p.wait()

    def correct_test_all(self, silent: 'bool'):
        correct_executables = os.listdir(config.BUILD_DIR)
        f = filter(lambda file: "correct" in file, correct_executables)
        correct_executables = list(f)
        for test in correct_executables:
            self.prepare_benchmark(silent)
            util.banner(test)
            self.do_benchmark(test, [])
            self.post_benchmark(silent)

    def restart_memcached_async(self, silent: 'bool'):
        # restart memcached
        util.state("restart memcached...")
        p = pm.launch_local_process_inline(
            "restart-memcached", ["./restart_memc.py"], silent)
        p.commit()
        self.__wait_process.append(p)

    def prepare_benchmark(self, silent: 'bool'):
        # 1) simulatneously execute these tasks
        build_p = util.rebuild_async(silent)
        self.kill_all_processes_async(silent)
        self.restart_memcached_async(silent)

        # manually joining build process
        # so that we can start deploy_sync-ing executables
        for line in build_p.stdout():
            print(line, end="")
            if config.BUILD_FAILED_MSG in line:
                util.error(
                    "** Possible build failed. Please check the log above")
                exit(-1)
        build_p.wait()
        util.state("build succeeded.")

        # 2) syncing files: ONLY after util.rebuild_async() finished
        self.deploy_sync_async(silent=True)

        self.__join()

    def post_benchmark(self, silent: 'bool'):
        self.fetch_logs_async(silent)
        self.fetch_results_async(silent)
        self.__join()

    def benchmark(self, binary: 'str', params: 'list[str]', hooks=[]):
        self.prepare_benchmark(True)
        self.do_benchmark(binary, params, hooks)
        self.post_benchmark(True)

    def __register_post_bench_callback(self):
        signal.signal(signal.SIGINT, self.__sigint_handler)

    def __sigint_handler(self, signum, frame):
        print("Clean up all the processes? (Y/n)", end=" ")
        s = input()
        util.error("Ctrl-C: exiting gracefully...")
        if s == "y" or s == "Y" or s == "":
            self.kill_all_processes_async(True)
        self.post_benchmark(True)
        exit(1)

    @util.report_time_decorator
    @util.report_compile_info_decorator
    def do_benchmark(self, binary: 'str', params: 'list[str]', hooks=[]):
        util.state(f"Build type: {util.query_cmake_build_type()}")
        self.cluster_mkdir(config.RESULT_DIR, True)

        self.__register_post_bench_callback()
        node_id = 1
        if len(config.NODES) > 1:
            for (ip, numa_id) in config.NODES[1:]:
                self.__execute_benchmark(
                    binary, params, ip, node_id, numa_id, True, hooks)
                node_id += 1

        ip, numa_id = config.NODES[0]
        node_id = 0
        self.__execute_benchmark(
            binary, params, ip, node_id, numa_id, False, hooks)
        self.__join()

    def __execute_benchmark(self, binary: 'str', params: 'list[str]', ip: 'str', node_id: 'str|int', numa_id: 'str|int', as_async: 'bool', hooks: 'list[str]'):
        '''
        @binary string
        @params [string]
        @ip string
        @node_id int, passed in as flag.
        @numa_id int, passed in as flag.
        @as_async bool
        - if as_async == True, the executable will not block the main process.
        - else, the executable will run in the foreground.
        @hooks [string]
        '''
        ssh_cmd = ['ssh', *config.SSH_FLAGS, f'root@{ip}']

        silent = as_async
        p = pm.launch_remote_process(binary, ip, silent)
        p.run(f"cd {config.BIN_DIR}")
        # correctly set environments...
        p.run("source /etc/profile")
        p.run("export TERM='linux'")
        p.run("export GLOG_logtostderr=1")
        p.run("export GLOG_colorlogtostderr=1")
        p.run("export ASAN_OPTIONS=halt_on_error=0")

        executable = os.path.join(config.BIN_DIR, binary)
        pass_params = ' '.join(params)
        exec_meta = util.generate_exec_meta(numa_id)
        log_path = os.path.join(config.BASE_DIR, f"{numa_id}.LOG")
        rnic = config.select_rnic(numa_id)
        redirect_log = ''
        if len(hooks) == 0:
            hooks = ['unbuffer']
        hook_command = ' '.join(hooks)
        if as_async:
            # if it is run asyncly, should rediret to a log file
            redirect_log = f"1>{log_path} 2>&1"
        else:
            # if it is run syncly, should print to console AND redirect to log file
            redirect_log = f"2>&1 |& tee {log_path}"
        script = f"{hook_command} {executable} {pass_params} --exec_meta='{exec_meta}' {redirect_log}"

        util.state(f"RUNNING {ip}({numa_id}): {script}")
        p.run(script)

        p.commit()
        if as_async:
            self.__wait_process.append(p)
        else:
            p.wait()

    def __join(self):
        for p in self.__wait_process:
            p.wait()
        self.__wait_process = []

    def cluster_bootstrap(self, silent: 'bool'):
        util.state("1) install dependencies")
        # expect for the `unbuffer` command
        installs = ['systemd-coredump',  'expect', 'llvm',
                    'valgrind', 'libgoogle-glog-dev',  'clang', 'clang-format', 'clang-format-10', 'libboost-all-dev']
        # for severless
        installs.append('libmagick++-dev')
        installs.append('linux-tools-generic')
        cmds = ['apt', 'install', '-y', *installs]
        self.cluster_execute_short_cmd(cmds, False)

        util.state("2) allocate huge pages")
        self.cluster_execute_short_cmd(
            ["echo", "32768", "> /proc/sys/vm/nr_hugepages"], silent)

        util.state("3) disable swap")
        self.cluster_execute_short_cmd(["swapoff", "-a"], silent)

    def cluster_tear_down(self):
        util.state("1) deallocate huge pages")
        self.cluster_execute_short_cmd(
            ["echo", "0", "> /proc/sys/vm/nr_hugepages"], False)

    @staticmethod
    def get_process_result(processes: 'list[pm.Process]') -> 'dict[str, list[str]]':
        '''
        @processes [(string, Popen)], a list of tuple with name and process
        '''
        outputs = {}
        for p in processes:
            p.commit()
            node = p.node()
            outputs[node] = []
            for line in p.stdout():
                outputs[node].append(line)
            p.wait()
        return outputs

    @staticmethod
    def show_process_result(results: 'dict[str, list[str]]'):
        '''
        @results map{string: [string]} , a map from name to list of lines
        '''
        for (name, lines) in results.items():
            util.banner(name)
            for line in lines:
                print(line, end="")
            print()
