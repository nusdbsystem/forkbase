#!/bin/bash

bin=`dirname $0`
bin=`cd $bin; pwd`

PERFMON_HOME=$bin/../

port=`grep "webserver_port=" $PERFMON_HOME/conf/perfmon.cfg | cut -d '=' -f 2`

cd $PERFMON_HOME/webui;
python -m SimpleHTTPServer $port;
