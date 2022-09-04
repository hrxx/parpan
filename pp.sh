#!/bin/bash
set -e

para_fun(){
# step1: split df
  mkdir _tmp_input
  mkdir _tmp_output
  mkdir _tmp_log
  python pp_split_df.py -if $1 -nc $2
  # step2: parallel run
  for (( i = 0; i < $2; i++ ));
  do
    python $4  -if _tmp_input/$i.parquet -of _tmp_output/$i.parquet > _tmp_log/$i.log 2>&1 & # add other arguments
  done
  # step3: merge
  python pp_merge_df.py -of $3 -nc $2
  rm -r _tmp_input
  rm -r _tmp_output
  rm -r _tmp_log
}

# step0: set variable
df_path=input.csv
num_cpu=4
output_file=result.csv
exe_script=app.py

#para_fun $df_path $num_cpu $output_file $exe_script # $other_augment
