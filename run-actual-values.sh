#!/bin/bash

#4,1
#8,1
#27,6
#64,16
#23,5
#30,7
#68,16
#30,6
#16,4
#71,16
#34,7
#27,5
#12,1
pushd /root/adam

export ADAM_EXECUTOR_MEMORY="28g"
export SPARK_HOME=/root/spark-old
export ADAM_HOME=/root/adam

function run_test {
  parts=$1
  mcs=$2
  # scale=`echo "$scale_base*5.0" | bc -l`
  # echo $scale
  
  num_workers=$mcs
  cp /root/spark-ec2/slaves /root/spark/conf/slaves
  $SPARK_HOME/sbin/stop-all.sh
  sleep 2
  head -n $num_workers /root/spark-ec2/slaves > $SPARK_HOME/conf/slaves
  $SPARK_HOME/sbin/start-all.sh
  sleep 5

  #./bin/adam-submit transform /NA12878.adam /NA12878.mkdup."$mcs".adam -mark_duplicate_reads 2>&1 | tee /mnt/logs/mkdup-"$mcs"-"$parts".log
  #
  #~/ephemeral-hdfs/bin/hadoop fs -rmr /NA12878.mkdup."$mcs".adam

  # ./bin/adam-submit flagstat /NA12878.adam 2>&1 | tee /mnt/logs/flagstat-"$mcs"-"$parts".log

  #./bin/adam-submit transform /NA12878.adam /NA12878.sort."$mcs".adam -sort_reads 2>&1 | tee /mnt/logs/sort-"$mcs"-"$parts".log
  #~/ephemeral-hdfs/bin/hadoop fs -rmr /NA12878.sort."$mcs".adam

  ./bin/adam-submit transform /NA12878.adam /NA12878.bqsr."$mcs".adam -recalibrate_base_qualities -known_snps /dbsnp_132.var.adam 2>&1 | tee /mnt/logs/bsqr-"$mcs"-"$parts".log
  ~/ephemeral-hdfs/bin/hadoop fs -rmr /NA12878.bsqr."$mcs".adam
}

run_test 1868 45
#run_test 1868 64
