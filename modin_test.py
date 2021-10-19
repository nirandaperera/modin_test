import os
import time

import ray

from ray.scripts import scripts

# ray.init(address='auto', _redis_password='5241590000000000')
ray.init(address='v-001:6379', _redis_password='5241590000000000', _node_ip_address='v-001')
# ray.init('auto')
# scripts.status()

os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray

num_rows = 100*10**6
print(num_rows)

import modin.pandas as pd
import numpy as np
frame_data = np.random.randint(0, num_rows, size=(num_rows, 2)) # 2GB each
frame_data1 = np.random.randint(0, num_rows, size=(num_rows, 2)) # 2GB each

df_l = pd.DataFrame(frame_data).add_prefix("col")
df_r = pd.DataFrame(frame_data1).add_prefix("col")

print("data reading completed")

# big_df = pd.concat([df for _ in range(2)]) # 20x2GB frames
# print(big_df)
# print(big_df.isna()) # The performance here represents a simple map
# print(big_df.groupby("col1").sum()) # group by on a large dataframe


t1 = time.time()
out = df_l.merge(df_r, on='col0', how='inner', suffixes=('_left', '_right'))
t2 = time.time()

print(f"###time {num_rows} {(t2 - t1) * 1000:.0f}ms, {out.shape[0]}", flush=True)
