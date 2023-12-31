#!/usr/bin/env bash
#
# start worker and client services
#

usage="Usage: ustore-start.sh [ arguments ]\n
        --config <configuration file> : if the configuration file is not in the default location (conf/config)\n"

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/ustore_env.sh
cd $USTORE_HOME

# create log directory
if [ ! -d $USTORE_LOG ]; then
  mkdir $USTORE_LOG
fi

if [ $# -ge 1 ]; then
    libs=$1
    echo "Using preload $libs"
    shift
    ustore_run="bin/run_preload.sh $libs bin/ustored $@"
else
    ustore_run="bin/ustored $@"
fi

# ssh and start ustore processes
ssh_options="-oStrictHostKeyChecking=no \
             -oUserKnownHostsFile=/dev/null \
             -oLogLevel=quiet"
ustore_sshrun="cd $USTORE_HOME; $ustore_run"

# start all the workers
echo "------------- Starting workers -------------"
for i in `cat $USTORE_CONF_HOST_FILE` ; do
  host=`echo $i | cut -d ':' -f 1`
  port=`echo $i | cut -d ':' -f 2`
  if [ $host = localhost ] ; then
    echo Starting worker @ $host : $ustore_run --node_id $i
    $ustore_run --node_id $i >> $USTORE_LOG/worker-$host-$port.log 2>&1 &
    sleep 1
  else
    echo Starting worker @ $host : $ustore_sshrun --worker --node_id $i
    ssh $ssh_options $host $ustore_sshrun --node_id $i >> $USTORE_LOG/worker-$host-$port.log 2>&1 &
    sleep 1
  fi
done

echo "----------- All processes started ------------"
