#!/usr/bin/python3

import exec
import itertools

inets = []
with open('inets.conf') as f:
    inets = f.read().strip().split('\n')
    inets = [item.strip() for item in inets]

def to_name(inet, pid):
    return '{}.pid={}'.format(inet, pid)
def init_conf(conf, inets, process_nr):
    for inet in inets:
        for pid in range(process_nr):
            name = to_name(inet, pid)
            conf.reg_machine(name, inet)


def gen_ib_send_bw_cmd(duration, qpn, size, post_list_size, cq_mod, port=18515, tx_depth=128, mr_per_qp=False, odp=False, use_hugepages=False, is_client=False, inline_size=0, inline_recv=0):
    cmd = [
        'unbuffer',
        'ib_send_bw',
        '--connection=RC',
        '--duration {}'.format(duration),
        '--qp {}'.format(qpn),
        '--size {}'.format(size),
        '--tx-depth {}'.format(tx_depth),
        '--post_list {}'.format(post_list_size),
        '--cq-mod {}'.format(cq_mod),
        '--port {}'.format(port),
        # '--inline_size 8', # soo slow after setting this
        # '--inline_recv 8', # sooo slow after setting this
        '--CPU-freq',
    ]
    if mr_per_qp:
        cmd.append("--mr_per_qp")
    if odp:
        cmd.append('--odp')
    if use_hugepages:
        cmd.append('--use_hugepages')
    if is_client:
        cmd = ['sleep 2;'] + cmd[:]
    if inline_size != 0:
        cmd.append('--inline_size {}'.format(inline_size))
    if inline_recv != 0:
        cmd.append('--inline_recv {}'.format(inline_recv))
    return cmd

def collect(map, name, p):
    if name not in map:
        map[name] = ''
    map[name] += p.stdout.read().decode("ascii")
    print("! collect {}. get {}".format(name, map[name]))

CSV_FILE = "fetched/ib_send_bw.csv"

if __name__ == '__main__':
    with open(CSV_FILE, "w") as f:
        f.write("tx-length,process_nr,qpn,size,inline,BW-average(MB/s),MsgRate(Mpps)\n")


    DURATION = 10
    DEFAULT_PORT = 18515
    # thread_nrs = [1, 2, 4, 6, 8, 10, 12, 14, 16, 32] # max at 10
    # thread_nrs = [8, 10, 12]
    thread_nrs = [1]
    tx_lengths = [128]
    qpns = [1]
    inlines = [True, False]
    # sizes = [1]
    # sizes = [1, 8, 28, 32, 36, 40, 48, 64]
    sizes = [16, 32]
    for (thread_nr, tx_length, qpn, size, inline) in itertools.product(thread_nrs, tx_lengths, qpns, sizes, inlines):
        conf = exec.Config()
        init_conf(conf, inets, thread_nr)

        for inet in inets:
            is_client = inet != inets[0]
            for pid in range(thread_nr):
                if inline:
                    inline_size = size
                    inline_recv = size
                else:
                    inline_size = 0
                    inline_recv = 0
                cmd = gen_ib_send_bw_cmd(DURATION, qpn, size, 64, 64, port=DEFAULT_PORT + pid, mr_per_qp=True, use_hugepages=True, is_client=is_client, inline_size=inline_size, inline_recv=inline_recv)
                if is_client:
                    cmd.append(' {}'.format(inets[0]))
                cmd.append(" |& tee {}.log".format(to_name(inet, pid)))
                conf.reg_cmd(to_name(inet, pid), cmd)


        executor = exec.Executor(conf)
        executor.run()
        executor.wait()
        sum_bw = 0
        sum_msg_rate = 0
        for id in conf.ids():
            out = executor.out(id)
            err = executor.err(id)

            if conf.machine(id) == inets[0]:
                continue
            try:
                ls = out.split('\n')
                byte_nr, iterations, bw_peak, bw_avg, msg_rate = ls[-3].split()
                sum_bw += float(bw_avg)
                sum_msg_rate += float(msg_rate)
            except:
                print("{}: Err when parsing output. stderr is \n{}".format(id, err))
        with open("fetched/ib_send_bw.csv", "a") as f:
            f.write('{}, {}, {},{},{}, {},{}\n'.format(
                tx_length, thread_nr, qpn, size, inline, sum_bw, sum_msg_rate))
        executor.to_file("fetched/{}".format(thread_nr))
        executor.print_err_if_exists()