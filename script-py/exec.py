class Executor:
    def __init__(self, machines):
        '''
        machines is a dict of (name, ip)
        '''
        self.machines = machines
        self.out = {}
        self.err = {}

    def run_cmd(self, cmd):
        for (name, machine) in self.machines.items():
            self.processes[name] = subprocess.Popen(
                [
                    'ssh',
                    machine,
                    *cmd
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def wait(self, time=1):
        while True:
            all_finish = True
            for (name, p) in self.processes.items():
                if p.poll() is None:
                    try:
                        p.wait(time)
                    except subprocess.TimeoutExpired:
                        all_finish = False
                        self.collect_output(name, p)
                else:
                    print("ERROR: handle p.poll() is not None")
                    exit(-1)
            if all_finish:
                break

    def collect_output(self, name, p):
        stdout, stderr = p.communicate()
        if name not in self.out:
            self.out[name] = ''
        if name not in self.err:
            self.err[name] = ''
        self.out[name] += stdout.read().decode('ascii')
        self.err[name] += stdout.read().decode('ascii')

    def to_file(self, dir):
        for name, out in self.out:
            with open(os.path.join(dir, name + ".stdout.log"), w) as f:
                f.write(out)
        for name, err in self.err:
            with open(os.path.join(dir, name + ".stdout.log"), w) as f:
                f.write(err)

    def out(self, name):
        return self.out[name]

    def err(self, name):
        return self.err[name]
