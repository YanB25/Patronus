#!/usr/bin/python3
import subprocess
import config
import util

DEBUG = False


class Process:
    def __init__(self, name: 'str', ip: 'str', init_cmds: 'list[str]', is_local: 'bool', silent: 'bool'):
        """
        @init_cmd [str] array of command to init the process
        @ip [str], not use if is_local is True
        @is_local boolean, whether it is a local process
        """
        self.__name = name
        self.__ip = ip
        self.__p = subprocess.Popen(
            init_cmds, stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, bufsize=0)
        self.__is_local = is_local
        self.__committed = False
        self.__silent = silent
        if DEBUG:
            print(
                f"[PM] Launching process {self.full_name()}: name {name} ip {ip} local {is_local} silent {silent} init_cmd {init_cmds}")

    def node(self):
        return self.__ip

    def stdout(self):
        assert self.__p.stdout is not None
        return self.__p.stdout

    def p(self):
        return self.__p

    def run(self, cmd: 'str'):
        if self.__committed:
            util.error(
                f"{self.full_name()}: Failed to run cmd {cmd}: already committed")
        if DEBUG:
            print(f"[PM] {self.full_name()}: run {cmd}")
        self.stdin().write(f"{cmd} \n")

    def commit(self):
        if DEBUG:
            print(f"[PM] {self.full_name()}: commit")
        if self.__committed:
            return
        if not self.__is_local:
            self.run("logout")
        self.stdin().close()
        self.__committed = True

    def full_name(self):
        return f"{self.__ip}@{self.__name}"

    def wait(self):
        if DEBUG:
            print(f"[PM] {self.full_name()}: wait")
        self.commit()
        if self.__silent:
            self.wait_handle_output(self.__do_nothing_handler)
        else:
            if DEBUG:
                print(f"Process {self.full_name()} ==>")
            self.wait_handle_output(self.__print_handler)
            # a new line here would be better
            print()
        if DEBUG:
            print(f"==> [PM] {self.full_name()}: leaved")

    def stdin(self):
        assert self.__p.stdin is not None
        return self.__p.stdin

    def wait_handle_output(self, handler):
        self.commit()
        for line in self.stdout():
            handler(line)
        self.__p.wait()

    def wait_retrieve_output(self):
        self.commit()
        ls = []
        for line in self.stdout():
            ls.append(line)
        return ls

    def __do_nothing_handler(self, line: 'str'):
        pass

    def __print_handler(self, line: 'str'):
        print(line, end='')

    def gen_retrive_handler(self, ls: 'list[str]'):
        def __retrive_handler(self, line):
            ls.append(line)
        return __retrive_handler


def launch_local_process(name: 'str', silent=False):
    init_cmds = ["/bin/bash"]
    return Process(name, 'localhost', init_cmds, True, silent)


def launch_remote_process(name: 'str', node: 'str', silent=False):
    init_cmds = ['ssh', '-tt', '-o LOGLEVEL=QUIET', f'root@{node}']
    return Process(name, node, init_cmds, False, silent)


def launch_local_process_inline(name: 'str', cmds: 'list[str]', silent=False):
    p = Process(name, 'localhost', cmds, True, silent)
    p.commit()
    return p


def launch_remote_process_inline(name: 'str', node: 'str', cmds: 'list[str]', silent=False):
    init_cmds = ['ssh', f'root@{node}', *cmds]
    p = Process(name, node, init_cmds, False, silent)
    p.commit()
    return p
