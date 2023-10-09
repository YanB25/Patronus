#!/usr/bin/python3

import subprocess
import pprint
import time

inets = []
with open('inets.conf') as f:
    inets = f.read().strip().split('\n')
    inets = [item.strip() for item in inets]


def gen_ib_write_bw_cmd(duration, qpn, size, tx_depth=128, mr_per_qp=False, odp=False, perform_warm_up=False, use_hugepages=False):
    cmd = [
        # 'unbuffer',
        'ib_write_bw',
        '--duration {}'.format(duration),
        '--qp {}'.format(qpn),
        '--size {}'.format(size),
        '--tx-depth {}'.format(tx_depth),
        '--CPU-freq'
    ]
    if mr_per_qp:
        cmd.append("--mr_per_qp")
    if odp:
        cmd.append('--odp')
    if perform_warm_up:
        cmd.append('--perform_warm_up')
    if use_hugepages:
        cmd.append('--use_hugepages')
    return cmd

def collect(map, name, p):
    if name not in map:
        map[name] = ''
    map[name] += p.stdout.read().decode("ascii")

def ib_write_bw(machines, duration, qpn, size, tx_depth=128, mr_per_qp=False):
    processes = {}
    process_texts = {}

    server = machines[0]
    clients = machines[1:]

    cmd = [
        'ssh',
        server,
        *gen_ib_write_bw_cmd(duration, qpn, size, tx_depth, mr_per_qp)
    ]

    pprint.pprint(' '.join(cmd))
    processes[server] = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    time.sleep(1)

    for machine in clients:
        cmd = [
            'ssh',
            machine,
            *gen_ib_write_bw_cmd(duration, qpn, size, tx_depth, mr_per_qp),
            server
        ]
        pprint.pprint(' '.join(cmd))
        processes[machine] = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    for (machine, p) in processes.items():
        if p.poll() is None:
            try:
                p.wait(1)
            except subprocess.TimeoutExpired:
                collect(process_texts, machine, p)
            collect(process_texts, machine, p)
    return process_texts


CSV_FILE = "fetched/ib_write_bw.csv"

if __name__ == '__main__':
    with open(CSV_FILE, "w") as f:
        f.write("tx-length,qpn,size,BW-average(MB/s),MsgRate(Mpps)\n")
    for machine in inets:
        # remove the file
        open("fetched/{}.log".format(machine), "w").close()

    DURATION = 5
    for tx_length in [1, 2, 4, 8]:
        for qpn in [1, 64, 256, 512, 1024, 2048, 4096, 8192]:
        # for qpn in [512]:
            # for size in [1, 64, 256, 4096]:
            for size in [512]:
                texts = ib_write_bw(inets, DURATION, qpn, size, tx_length)
                sum_bw = 0
                sum_msg_rate = 0
                for (machine, text) in texts.items():
                    with open("fetched/{}.log".format(machine), "a") as f:
                        f.write(text)
                    if machine == inets[0]:
                        continue
                    ls = text.split('\n')
                    byte_nr, iterations, bw_peak, bw_avg, msg_rate = ls[-3].split()
                    sum_bw += float(bw_avg)
                    sum_msg_rate += float(msg_rate)
                    with open("fetched/ib_write_bw.csv", "a") as f:
                        f.write('{}, {},{},{},{}\n'.format(
                            tx_length, qpn, size, sum_bw, sum_msg_rate))
