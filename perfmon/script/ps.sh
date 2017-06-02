#!/usr/bin/env bash

if [ $# != 0 ]; then
    echo "usage: ./ps.sh"
    exit
fi

bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

dest=`grep "pid_dir=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`

if [ ! -d $dest ]; then
  mkdir $dest
fi

ps -aux | grep ustored | grep -v "grep" | awk '{print $2}' > $dest/worker
