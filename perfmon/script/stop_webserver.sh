#!/bin/bash
bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

port=`grep "webserver_port=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`

echo "stoppint web server"
ps -aux | grep "python -m SimpleHTTPServer $port" | awk '{print $2}' | xargs kill -9
