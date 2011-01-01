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

# start dfs daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts
export HADOOP_COMMON_HOME=`exec pwd`
"$HADOOP_COMMON_HOME"/bin/hadoop-daemons.sh --script "$bin"/piccolo stop piccolo stop
