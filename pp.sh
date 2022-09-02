#!/bin/bash
set -e
# step0: set variable
df_path=input.csv
num_cpu=4
output_file=result.csv
# step1: split df
mkdir _tmp_input
mkdir _tmp_output
python split_df.py $df_path $num_cpu
# step2: parallel run
for i in {0..3}
do
  python app.py $i
done
# step3: merge
python merge_df.py $output_file $num_cpu
rm -r _tmp_input
rm -r _tmp_output