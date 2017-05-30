#!/usr/bin/env bash
#
# stop worker services
#

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/ustore_env.sh
cd $USTORE_HOME

ssh_options="-oStrictHostKeyChecking=no \
             -oUserKnownHostsFile=/dev/null \
             -oLogLevel=quiet"
ustore_kill="killall -q -s SIGTERM -r ustored"
ustore_fkill="killall -q -s SIGKILL -r ustored"
httpc_kill="killall -q -s SIGTERM -r ustore_http"
httpc_fkill="killall -q -s SIGKILL -r ustore_http"

# kill ustore worker processes
host_file=$USTORE_CONF/workers
hosts=`cat $host_file | cut -d ':' -f 1`
for i in ${hosts[@]}; do
  echo Stop ustore @ $i ...
  if [ $i == localhost ]; then
    $ustore_kill
    sleep 1
    $ustore_fkill
  else
    ssh $ssh_options $i $ustore_kill
    sleep 1
    $ustore_fkill
  fi
done

# kill ustore http client service
echo Stop http client ...
$httpc_kill
sleep 1
$httpc_fkill
echo "----------- All processes stopped ------------"
