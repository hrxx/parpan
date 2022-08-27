import os
import sys
import subprocess
import math
import time
import pandas as pd

class ParallelDF:
    def __init__(self, df=None, fun=None):
        self.whole_df = df
        self.process_fun = fun.__name__
        self.num_cpu = 4
        self.tmp_input_path = '_tmp_input/{}.parquet'
        self.tmp_output_path = '_tmp_output/{}.parquet'
        self.file_name = 'split_part_'
        self.python_path = sys.executable
        self.script_path = '_tmp_execute.py'
        self.script = '''
import pandas as pd
from app import {fun_name}
import sys
print(sys.argv)
idx = (sys.argv[1])
df = pd.read_parquet('_tmp_input/'+str(idx)+'.parquet')
df = {fun_name}(df)
df.to_parquet('_tmp_output/'+str(idx)+'.parquet')
'''.format(fun_name=self.process_fun)

    def init(self):
        os.system('mkdir _tmp_input')
        os.system('mkdir _tmp_output')
        fw_script = open(self.script_path,'w')
        fw_script.write(self.script)

    def release(self):
        os.system('rm -r _tmp_input')
        os.system('rm -r _tmp_output')

    def split_df(self):
        df_len = math.ceil(len(self.whole_df) / self.num_cpu)
        for i in range(self.num_cpu):
            self.whole_df[i*df_len:(i+1)*df_len].to_parquet(self.tmp_input_path.format(i))
        return

    def merge_df(self):
        lst_df = [pd.read_parquet(self.tmp_output_path.format(i)) for i in range(self.num_cpu)]
        return pd.concat(lst_df)

    def parallel_run(self):
        print('initial env.....')
        self.init()
        print('split dataframe.....')
        self.split_df()
        print('starting process.....')
        lst_process = list()
        for i in range(self.num_cpu):
            process = subprocess.Popen(self.python_path+ ' ' + self.script_path + ' ' + str(i), shell=True)
            lst_process.append(process)
        [p.wait() for p in lst_process]
        print('merge result........')
        res = self.merge_df()
        print('release resources........')
        self.release()
        return res
