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
httpc_kill="killall -q -w -s SIGTERM -r ustore_http"
httpc_fkill="killall -q -s SIGKILL -r ustore_http"

# kill ustore worker processes
hosts=`cat $USTORE_CONF_HOST_FILE | cut -d ':' -f 1`
for i in ${hosts[@]}; do
  echo Stop ustore @ $i ...
  if [ $i == localhost ]; then
    ./bin/ustore_stop_worker.sh
  else
    ssh $ssh_options $i $USTORE_HOME/bin/ustore_stop_worker.sh
  fi
done

# kill ustore http client service
echo Stop http client ...
$httpc_kill > /dev/null 2>&1 &
kpid=$!
wait_and_fkill "$httpc_fkill" $kpid 1
echo "----------- All processes stopped ------------"
