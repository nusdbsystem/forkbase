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

echo "----------- All processes stopped ------------"
