import subprocess
import pprint
import os
import time

class Config:
    def __init__(self):
        # @id to @ip that can use for ssh connection
        self.__machines = {}
        # @id to @cmd that is run
        self.__cmds = {}
        # @id to @private_data that user can attach. Config ignores this field
        self.__priv = {}
    def reg_machine(self, id, machine):
        self.__machines[id] = machine
    def reg_machines(self, ids, machines):
        for (id, machine) in zip(ids, machines):
            self.reg_machine(id, machine)
    def reg_cmd(self, id, cmd):
        self.__cmds[id] = cmd
    def reg_cmds(self, ids, cmds):
        for (id, cmd) in zip(ids, cmds):
            self.reg_cmd(id, cmd)
    def reg_machine_cmd(self, id, machine, cmd):
        self.reg_machine(id, machine)
        self.reg_cmd(id, cmd)
    def show(self):
        pprint.pprint(self.__machines)
        pprint.pprint(self.__cmds)
    def reg_priv(self, id, data):
        self.__priv[id] = data
    
    def cmd(self, id):
        return self.__cmds[id]
    def ids(self):
        return self.__machines.keys()
    def machine(self, id):
        return self.__machines[id]
    def priv(self, id):
        return self.__priv[id]


class Executor:
    def __init__(self, conf):
        '''
        machines is a dict of (@name, @ip)
        @name is the unique string to identify a machine.
        @ip is the address used for ssh connection.
        '''
        self.__conf = conf
        self.__out = {}
        self.__err = {}
        self.__processes = {}

    def run(self):
        for id in self.__conf.ids():
            machine = self.__conf.machine(id)
            cmd = self.__conf.cmd(id)
            pprint.pprint(cmd)
            self.__processes[id] = subprocess.Popen(
                [
                    'ssh',
                    machine,
                    *cmd,
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            

    def wait(self, timeout=30):
        '''
        call me to wait until all the process finishes
        '''
        fast_failure_rollback = False
        for (name, p) in self.__processes.items():
            try:
                stdout, stderr = p.communicate(input=None, timeout=0 if fast_failure_rollback else timeout)
                self.collect_output(name, stdout, stderr)
            except subprocess.TimeoutExpired:
                fast_failure_rollback = True
                p.kill()
                stdout, stderr = p.communicate(input=None, timeout=None)
                self.collect_output(name, stdout, stderr)

    def collect_output(self, name, stdout, stderr):
        if name not in self.__out:
            self.__out[name] = ''
        if name not in self.__err:
            self.__err[name] = ''
        self.__out[name] += stdout.decode('ascii')
        self.__err[name] += stderr.decode('ascii')
        if (len(self.__err[name]) > 0):
            print("{} ERR: {}".format(name, self.__err[name]))

    def to_file(self, dir):
        if not os.path.exists(dir):
            os.mkdir(dir)
        for name, out in self.__out.items():
            with open(os.path.join(dir, name + ".stdout.log"), 'w') as f:
                f.write(out)
        for name, err in self.__err.items():
            with open(os.path.join(dir, name + ".stderr.log"), 'w') as f:
                f.write(err)

    def out(self, name):
        return self.__out[name]

    def err(self, name):
        return self.__err[name]
    
    def print_err_if_exists(self):
        for id, err in self.__err.items():
            if len(err) != 0:
                print("{} ERR: {}".format(id, err))