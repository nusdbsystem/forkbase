#! /usr/bin/env bash
bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

PID_DIR=`grep "pid_dir=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`

# start pm_daemon in the current node
mkdir -p $PERFMON_HOME/logs
rm -rf $PID_DIR
mkdir -p $PID_DIR
cd $PERFMON_HOME
echo "starting pm_daemon"
./build/pm_daemon >> ./logs/pm_daemon.log 2>&1 &
echo "starting web server"
./script/start_webserver.sh >> ./logs/pm_webserver.log 2>&1 &
sleep 1

# slave pm_monitor in each slave node
host_file=$PERFMON_HOME/script/slaves
for host in `cat $host_file` ; do
    echo "starting pm_monitor @ $host"
    ssh $host "cd $PERFMON_HOME; rm -rf $PID_DIR; mkdir -p $PID_DIR; ./script/ps.sh; ./build/pm_monitor" >> $PERFMON_HOME/logs/pm_monitor-$host.log 2>&1 & 
done
