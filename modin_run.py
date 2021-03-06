import os


import time
import argparse
import math
import subprocess
import os 
import gc
import numpy as np
import pandas as pd 
import datetime

parser = argparse.ArgumentParser(description='generate random data')

# parser.add_argument('-s', dest='scale', type=str, help='number of rows', required=True)
parser.add_argument('-w', dest='world', type=int, help='processes', required=True, nargs='+')
parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)

args = parser.parse_args()
args = vars(args)
print(args, flush=True)

# scale = args['scale']
world = args['world']
rows = args['rows']
it = args['it']

TOTAL_NODES = 10
RAY_PW = '1234'
RAY_EXEC = "/N/u2/d/dnperera/victor/MODIN/bin/ray"

nodes_file = "nodes.txt"
ips = []

with open(nodes_file, 'r') as fp:
    for l in fp.readlines():
        ips.append(l.split(' ')[0])


# ray start --head --redis-port=6379 --node-ip-address=v-001
def start_ray(procs, nodes):
    print("starting head", flush=True)
#     query = ["ssh", "v-001", RAY_EXEC, "start",
#              "--head", "--redis-port=6379", "--node-ip-address=v-001",
#              f"--redis-password={RAY_PW}", f"--num-cpus={procs}",
# #              f"--memory={20 * procs * (10 ** 9)}"]
# #              "--resources={\"memory\":" + str(20 * procs * 10 ** 9) +"}"
#             ]

    query = f"ssh v-001 {RAY_EXEC} start --head --port=6379 --node-ip-address=v-001 --redis-password={RAY_PW} --num-cpus={procs}"           
    print(f"running: {query}", flush=True)
    subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)

    time.sleep(3)

    for ip in ips[1:nodes]:
        # print(f"starting worker {ip}", flush=True)
#         query = ["ssh", ip, RAY_EXEC, "start",
#                  "--redis-address=\'v-001:6379\'", f"--node-ip-address={ip}",
#                  f"--redis-password={RAY_PW}", f"--num-cpus={procs}",
# #                  f"--memory={20 * procs * 10 ** 9}"]
# #                  "--resources={\"memory\":" + str(20 * procs * 10 ** 9) +"}"
#                 ]
        query = f"ssh {ip} {RAY_EXEC} start --address=\'v-001:6379\' --node-ip-address={ip} --redis-password={RAY_PW} --num-cpus={procs}"
        print(f"running: {query}", flush=True)
        subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)

    time.sleep(3)


def stop_ray():
    import ray
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in ips:
        subprocess.run(f"ssh {ip} {RAY_EXEC} stop", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    # time.sleep(5)

    
if __name__ == "__main__":
    os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray

    timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

    for r in rows:
        frame_data = np.random.randint(0, r, size=(r, 2)) 
        frame_data1 = np.random.randint(0, r, size=(r, 2)) 
        print(f"data generated", flush=True)
        
        for w in world:
            procs = int(math.ceil(w / TOTAL_NODES))
            print(f"world sz {w} procs per worker {procs} iter {it}", flush=True)

            assert procs <= 16

            try:
                stop_ray()
                start_ray(procs, min(w, TOTAL_NODES))
                
                import ray
                ray.init(address='v-001:6379', _redis_password=RAY_PW, _node_ip_address='v-001')

                import modin.pandas as pd
                import modin.config
                # pd.DEFAULT_NPARTITIONS = w
                modin.config.NPartitions.put(w)
            
                for i in range(it):

                    df_l = pd.DataFrame(frame_data).add_prefix("col")
                    df_r = pd.DataFrame(frame_data1).add_prefix("col")
                    print(f"data loaded", flush=True)


                    t1 = time.time()
                    out = df_l.merge(df_r, on='col0', how='inner', suffixes=('_left', '_right'))
                    t2 = time.time()

                    # timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

                    timing['rows'].append(r)
                    timing['world'].append(w)
                    timing['it'].append(i)
                    timing['time'].append((t2 - t1) * 1000)
                    print(f"timings {r} {w} {i} {(t2 - t1) * 1000:.0f} ms, {out.shape[0]}", flush=True)
                    
                    del df_l 
                    del df_r
                    del out 
                    gc.collect()
            finally:
                stop_ray()
                
    pd.DataFrame(timing).to_csv(f'timing-{datetime.datetime.now().isoformat()}.csv', index=False)