#!/bin/bash
set -e
# when use this file
# pls comment the last executable line in pp.sh
source ./pp.sh

for ln in 1 2 3
do
  para_fun $df_path $num_cpu $output_file $exe_script
done

#'hdfs:///projects/mpi_clsfspu/hive/dev_mpi_clsfspu/tmp_dwd_attribute_to_check_with_cate_id_hrx/global_be_category_id={}'.format(leafnode_id)