import pandas as pd
import sys
import math
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-if', '--input_file', type=str, required=True, default='_tmp_input/0.parquet')
parser.add_argument('-nc', '--num_cpu', type=int, required=True, default=4)
args = parser.parse_args()
input_file = str(args.input_file)
num_cpu = int(args.num_cpu)

print('input file: {} reading....'.format(input_file))
if 'csv' in input_file:
    whole_df = pd.read_csv(input_file)
else:
    whole_df = pd.read_parquet(input_file)
print('read data done. start to split dataframe......')
df_len = math.ceil(len(whole_df) / num_cpu)
for i in range(num_cpu):
    whole_df[i*df_len:(i+1)*df_len].to_parquet('_tmp_input/{}.parquet'.format(i))
print('split done')
