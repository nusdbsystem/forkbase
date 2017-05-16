#! /usr/bin/env bash
bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

# slave pm_monitor in each slave node
host_file=$PERFMON_HOME/script/slaves
for host in `cat $host_file` ; do
  echo "stopping pm_monitor @ $host"
  if [ $host == localhost ]; then
    killall pm_monitor
  else
    ssh $host "killall pm_monitor"
  fi
done

sleep 1
echo "stopping pm_daemon"
killall pm_daemon

cd $PERFMON_HOME
./script/stop_webserver.sh
