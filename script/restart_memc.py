#!/usr/bin/python3
import config
from cluster import Cluster
import util

c = Cluster(config.NODES)

silent = not config.DEBUG_SCRIPT

ip, port = config.get_memcached_conf()

# kill the old memcached process
c.node_execute_short_cmd(ip,
                         ['cat', '/tmp/memcached.pid', '|', 'xargs', 'kill'],
                         silent
                         )

# start a new memcached process
c.node_execute_short_cmd(ip,
                         ['memcached', '-u root', f"-l {ip}",
                             f"-p {port}", '-c 10000', '-d', '-P /tmp/memcached.pid'],
                         silent
                         )

# init serverNum and clientNum to 0
p1 = c.node_execute_short_cmd_async(ip,
["echo", "-e", "'set serverNum 0 0 1\\r\\n0\\r\\nquit\\r'", "|", "nc", ip, port],
    True)
p2 = c.node_execute_short_cmd_async(ip,
["echo", "-e", "'set clientNum 0 0 1\\r\\n0\\r\\nquit\\r'", "|", "nc", ip, port],
    True)

p1.commit()
succeed1 = False
for line in p1.stdout():
    if "STORED" in line:
        succeed1 = True
p1.wait()

p2.commit()
succeed2 = False
for line in p2.stdout():
    if "STORED" in line:
        succeed2 = True
p2.wait()

if not succeed1 or not succeed2:
    util.error(f"Failed to set serverNum to memcached at {ip}:{port}")
    exit(-1)
util.state(f"Restart memcached succeeded at {ip}:{port}")