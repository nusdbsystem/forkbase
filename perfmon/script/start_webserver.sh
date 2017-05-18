#!/bin/bash

bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

port=`grep "webserver_port=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`
monitor_port=`grep "http_port=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`
host=`grep "hostname=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`

sed -i "6c\ \ Monitor.API=\"http://$host:$monitor_port\";" $PERFMON_HOME/webui/js/monitor.js
cd $PERFMON_HOME/webui;

python -m SimpleHTTPServer $port;
