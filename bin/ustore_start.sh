#!/usr/bin/env bash
#
# start the workers and client services
#

usage="Usage: ustore-run.sh [ arguments ]\n
        --config <configuration file> : if the configuration file is not in the default location (conf/config)\n"

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/ustore_env.sh

# create log directory
if [ ! -d $USTORE_LOG ]; then
  mkdir $USTORE_LOG
fi

# go to ustore home to execute binary
cd $USTORE_HOME

# ssh and start ustore processes
ssh_options="-oStrictHostKeyChecking=no \
             -oUserKnownHostsFile=/dev/null \
             -oLogLevel=quiet"
ustore_run="bin/ustored $@"
ustore_sshrun="cd $USTORE_HOME; $ustore_run"

# start all the workers
host_file=$USTORE_CONF/workers
# old_IFS=$IFS
# IFS=$'\n'
for i in `cat $host_file` ; do
  host=`echo $i | cut -d ':' -f 1`
  port=`echo $i | cut -d ':' -f 2`
  if [ $host = localhost ] ; then
    echo Starting worker @ $host : $ustore_run --node_id $i
    $ustore_run --node_id $i > $USTORE_LOG/worker-$host-$port.log 2>&1 &
  else
    echo Starting worker @ $host : $ustore_sshrun --worker --node_id $i
    ssh $ssh_options $host $ustore_sshrun --node_id $i >> $USTORE_LOG/worker-$host-$port.log 2>&1 &
  fi
done
echo "-------------Started all the workers------------"
echo ""

## start all the client services
#host_file=$USTORE_HOME/conf/client_services
#for i in `cat $host_file` ; do
#  host=`echo $i | cut -d ':' -f 1`
#  port=`echo $i | cut -d ':' -f 2`
#  if [ $host = localhost ] ; then
#    echo Starting client service @ $host : $ustore_run --client_service --node_id $i
#    $ustore_run --client_service --node_id $i > $USTORE_LOG/client-service-$host-$port.log 2>&1 &
#  else
#    echo Starting client service @ $host : $ustore_sshrun --client_service --node_id $i
#    ssh $ssh_options $host "$ustore_sshrun --client_service --node_id $i" >> $USTORE_LOG/client-service-$host-$port.log 2>&1 &
#  fi
#done
#echo "---------Started all the client services---------"
## IFS=$old_IFS
