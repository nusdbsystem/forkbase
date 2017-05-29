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
ustore_kill="killall -q -s SIGKILL -r ustored"

# kill ustore worker processes
host_file=$USTORE_CONF/workers
hosts=`cat $host_file | cut -d ':' -f 1`
for i in ${hosts[@]}; do
  echo Kill ustore @ $i ...
  if [ $i == localhost ]; then
    $ustore_kill
  else
    ssh $ssh_options $i $ustore_kill
  fi
done
echo "----------- All workers stopped ------------"

## kill ustore client service processes
#host_file=$USTORE_CONF/client_services
#hosts=`cat $host_file | cut -d ':' -f 1`
#for i in ${hosts[@]}; do
#  echo Kill ustore @ $i ...
#  if [ $i == localhost ]; then
#    $ustore_kill
#  else
#    ssh $ssh_options $i $ustore_kill
#  fi
#done

# wait for killall command
sleep 2
