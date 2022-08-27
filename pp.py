import os
import sys
import subprocess
import math
import time
import pandas as pd


def parallel_df(f):
    num_cpu = 4
    tmp_input_path = '_tmp_input/{}.parquet'
    tmp_output_path = '_tmp_output/{}.parquet'
    file_name = 'split_part_'
    python_path = sys.executable
    script_path = '_tmp_execute.py'
    script = '''
import pandas as pd
from app import {fun_name}
import sys
print(sys.argv)
idx = (sys.argv[1])
df = pd.read_parquet('_tmp_input/'+str(idx)+'.parquet')
df = {fun_name}(df)
df.to_parquet('_tmp_output/'+str(idx)+'.parquet')
'''

    def init():
        os.system('mkdir _tmp_input')
        os.system('mkdir _tmp_output')
        fw_script = open(script_path,'w')
        fw_script.write(script.format(fun_name='g'))


    def release():
        os.system('rm -r _tmp_input')
        os.system('rm -r _tmp_output')

    def split_df(whole_df):
        df_len = math.ceil(len(whole_df) / num_cpu)
        for i in range(num_cpu):
            whole_df[i*df_len:(i+1)*df_len].to_parquet(tmp_input_path.format(i))
        return

    def merge_df():
        lst_df = [pd.read_parquet(tmp_output_path.format(i)) for i in range(num_cpu)]
        return pd.concat(lst_df)

    def wrap_fun(*args, **kwargs):
        init() # TODO: create script
        split_df(*args)
        print('split dataframe.....')
        lst_process = list()
        for i in range(num_cpu):
            process = subprocess.Popen(python_path+ ' ' + script_path + ' ' + str(i), shell=True)
            lst_process.append(process)
        [p.wait() for p in lst_process]
        print('merge result........')
        res = merge_df()
        release()
        return res
    return wrap_fun