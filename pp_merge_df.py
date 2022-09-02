import pandas as pd
import sys
import time
import glob
output_file = sys.argv[1]
num_cpu = int(sys.argv[2])

while (True):
    files = glob.glob('_tmp_output/*parquet')
    print('output file num: ', len(files))
    if len(files) == num_cpu:
        break
    time.sleep(5)

lst_df = [pd.read_parquet('_tmp_output/{}.parquet'.format(i)) for i in range(num_cpu)]
pd.concat(lst_df).to_parquet(output_file)
print('merge done and saved')
print(output_file)
print('='*200)
