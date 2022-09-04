import pandas as pd
import sys
import time
import glob
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-of', '--output_file', type=str, required=True, default='_tmp_output/0.parquet')
parser.add_argument('-nc', '--num_cpu', type=int, required=True, default=4)
parser.add_argument('-rd', '--remove_duplicate', type=bool, required=False, default=False)
args = parser.parse_args()
output_file = str(args.output_file)
num_cpu = int(args.num_cpu)
remove_duplicate = bool(args.remove_duplicate)

while (True):
    files = glob.glob('_tmp_output/*parquet')
    print('output file num: ', len(files))
    if len(files) == num_cpu:
        break
    time.sleep(5)

lst_df = [pd.read_parquet('_tmp_output/{}.parquet'.format(i)) for i in range(num_cpu)]
df_whole = pd.concat(lst_df)

if remove_duplicate:
    print('drop duplicates')
    df_whole = df_whole.drop_duplicates()

if 'csv' in output_file:
    df_whole.to_csv(output_file, index=False)
else:
    df_whole.to_parquet(output_file)

print('merge done and saved')
print(output_file)
print('='*200)
