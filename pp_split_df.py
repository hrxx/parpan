import pandas as pd
import sys
import math
import glob
path = sys.argv[1]
num_cpu = int(sys.argv[2])

def read_parquet_folder(file_path_with_prefix):
    files = glob.glob(file_path_with_prefix)
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs)

# whole_df = pd.read_csv(path)
whole_df = read_parquet_folder(path+'/2022*')
print('read data done. start to split dataframe......')
df_len = math.ceil(len(whole_df) / num_cpu)
for i in range(num_cpu):
    whole_df[i*df_len:(i+1)*df_len].to_parquet('_tmp_input/{}.parquet'.format(i))
print('split done')
