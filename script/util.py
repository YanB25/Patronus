#!/usr/bin/python3
import subprocess
import pm
import os
import config
import time


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def check_output(cmd: 'list[str]'):
    return subprocess.check_output(cmd).decode('utf-8').strip()


def generate_exec_meta(numa_id):
    commit_hash = check_output(["git", "rev-parse", "HEAD"])
    short_hash = commit_hash[:8]
    date = check_output(["date", "+%Y-%m-%d.%H:%M:%S"])
    exec_meta = f"{date}.{short_hash}.{numa_id}"
    return exec_meta


def rebuild_async(silent: 'bool'):
    p = pm.launch_local_process("rebuild", silent)
    p.run("source /etc/profile \n")
    p.run(f"export CC=$(which {config.CC}) \n")
    p.run(f"export CXX=$(which {config.CXX}) \n")
    p.run("mkdir -p ../build \n")
    p.run("cd ../build \n")
    p.run("cmake .. \n")
    p.run("make -j \n")
    p.run("cd ../script \n")
    p.commit()
    return p


def rebuild():
    p = rebuild_async(False)
    p.wait()


def build(build_type: 'str', silent: 'bool'):
    '''
    @build_type string. One of "Debug" or "Release"
    '''
    p = pm.launch_local_process("build", silent)
    p.run("source /etc/profile \n")
    p.run(f"export CC=$(which {config.CC}) \n")
    p.run(f"export CXX=$(which {config.CXX}) \n")
    p.run("mkdir -p ../build \n")
    p.run("cd ../build \n")
    p.run("rm -rf * \n")
    p.run(f"cmake -DCMAKE_BUILD_TYPE={build_type} .. \n")
    p.run("make clean \n")
    p.run("make -j \n")
    p.run("cd ../script \n")
    p.commit()

    p.wait()


def build_debug(silent: 'bool'):
    return build("Debug", silent)


def build_release(silent):
    return build("Release", silent)


def chunk(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def listdir(dir, full=False):
    '''
    Like os.listdir, but
    @full bool, whether return full paths
    '''
    ret = os.listdir(dir)
    if full:
        ret = [os.path.join(dir, file) for file in ret]
    return ret


def is_valid_ip(address: 'str'):
    parts = address.split(".")
    if len(parts) != 4:
        return False
    for item in parts:
        if not 0 <= int(item) <= 255:
            return False
    return True


def is_valid_port(port: 'str|int'):
    port = int(port)
    return port > 1 and port < 65535


def banner(msg):
    return print(f"{bcolors.HEADER}================= {msg} ================={bcolors.ENDC}")


def state(msg):
    return print(f"{bcolors.BOLD}{bcolors.OKGREEN}{msg}{bcolors.ENDC}")


def error(msg):
    return print(f"{bcolors.FAIL}{msg}{bcolors.ENDC}")


def warning(msg):
    return print(f"{bcolors.WARNING}{msg}{bcolors.ENDC}")


def query_cmake(field: 'str'):
    cmake_cache_file = os.path.join(config.BUILD_DIR, "CMakeCache.txt")
    try:
        with open(cmake_cache_file) as f:
            for line in f:
                if field in line:
                    name, others = line.strip().split(":")
                    t, value = others.strip().split("=")
                    return value
    except Exception as e:
        error(f"Failed to query cmake for {field}: {e}")


def query_cmake_build_type():
    return query_cmake("CMAKE_BUILD_TYPE")


def query_build_flags(flag: 'str') -> 'bool':
    flags_path = os.path.join(
        config.BUILD_DIR, "CMakeFiles/sherman.dir/flags.make")
    with open(flags_path) as f:
        for line in f:
            if flag in line:
                return True
    return False


def query_asan_enabled():
    return query_build_flags("-fsanitize")


def report_compile_info():
    build_type = query_cmake_build_type()
    if build_type != "Release":
        warning(f"Build type: {build_type}")
    else:
        state(f"Build type: {build_type}")
    asan_enabled = query_asan_enabled()
    if asan_enabled:
        warning(f"Asan enabled: True")
    else:
        state(f"Asan enabled: False")
    no_omit_frame_ptr = query_build_flags("-fno-omit-frame-pointer")
    if no_omit_frame_ptr:
        warning(f"Frame pointer: no omit")
    ndebug = query_build_flags("-DNDEBUG")
    if not ndebug:
        warning(f"NDEBUG: not defined")


def report_time_decorator(func):
    def wrapper_function(*args, **kwargs):
        start = time.time()
        func(*args,  **kwargs)
        end = time.time()
        elaps = end - start
        state("Approximately take {:.4}s".format(elaps))
    return wrapper_function


def report_compile_info_decorator(func):
    def wrapper_function(*args, **kwargs):
        func(*args,  **kwargs)
        report_compile_info()
    return wrapper_function
