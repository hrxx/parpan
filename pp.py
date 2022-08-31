import os
import sys
import subprocess
import math
import time
import pandas as pd
import __main__
import glob

class ParallelDF:
    def __init__(self, df=None, fun=None):
        self.whole_df = df
        self.process_fun = fun.__name__
        # self.module_name = os.path.basename(__main__.__file__).strip(".py")
        self.module_name = __main__.__file__[__main__.__file__.rfind('/') + 1:-3]
        self.num_cpu = 10
        self.tmp_input_path = '_tmp_input/{}.parquet'
        self.tmp_output_path = '_tmp_output/{}.parquet'
        self.file_name = 'split_part_'
        self.python_path = sys.executable
        self.script_path = '_tmp_execute.py'
        self.script = '''
import pandas as pd
from {module_name} import {fun_name}
import sys
print(sys.argv)
idx = (sys.argv[1])
df = pd.read_parquet('_tmp_input/'+str(idx)+'.parquet')
df = {fun_name}(df)
df.to_parquet('_tmp_output/'+str(idx)+'.parquet')
'''.format(fun_name=self.process_fun, module_name=self.module_name)

    def init(self):
        os.system('mkdir _tmp_input')
        os.system('mkdir _tmp_output')
        os.system('mkdir _tmp_log')
        fw_script = open(self.script_path, 'w')
        fw_script.write(self.script)

    def release(self):
        os.system('rm -r _tmp_input')
        os.system('rm -r _tmp_output')
        os.system('rm -r _tmp_log')

    def split_df(self):
        df_len = math.ceil(len(self.whole_df) / self.num_cpu)
        for i in range(self.num_cpu):
            self.whole_df[i * df_len:(i + 1) * df_len].to_parquet(self.tmp_input_path.format(i))
        self.whole_df = None
        del(self.whole_df)
        return

    def merge_df(self):
        lst_df = [pd.read_parquet(self.tmp_output_path.format(i)) for i in range(self.num_cpu)]
        return pd.concat(lst_df)

    def subprocess_method(self):
        lst_process = list()
        for i in range(self.num_cpu):
            process = subprocess.Popen(
                self.python_path + ' ' + self.script_path + ' ' + str(i) + ' > _tmp_log/' + str(i) + ' 2>&1',
                shell=True)
            lst_process.append(process)
        [p.wait() for p in lst_process]  # not work well, need more memory
        return

    def shell_method(self):
        for i in range(self.num_cpu):
            os.system(
                self.python_path + ' ' + self.script_path + ' ' + str(i) + ' > _tmp_log/' + str(i) + ' 2>&1 &')
        while (True):
            files = glob.glob('_tmp_output/*parquet')
            print('output file num: ', len(files))
            if len(files) == self.num_cpu:
                break
            time.sleep(5)
        return

    def parallel_run(self):
        print('initial env.....')
        self.init()
        print('split dataframe.....')
        self.split_df()
        print('starting process.....')
        self.subprocess_method()
        print('merge result........')
        res = self.merge_df()
        print('release resources........')
        self.release()
        return res
