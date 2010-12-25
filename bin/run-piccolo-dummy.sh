#!/usr/bin/env bash

# Start piccolo daemons.
# Optinally upgrade or rollback dfs state.
# Run this on master node.

usage="Usage: start-piccolo.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/hdfs-config.sh

# get arguments
if [ $# -ge 1 ]; then
	echo $usage
	exit 1
fi

# run piccolo-dummy
export HADOOP_COMMON_HOME=`exec pwd`
"$HADOOP_COMMON_HOME"/bin/hadoop-daemons.sh --script "$bin"/piccolo-dummy start piccolo-dummy
#sleep 5
#echo "woke up"
#"$HADOOP_COMMON_HOME"/bin/hadoop-daemons.sh --script "$bin"/piccolo-dummy stop piccolo-dummy