#!/usr/bin/env bash
#
# stop worker services and clean data
#

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/ustore_env.sh
cd $USTORE_HOME

# stop service
./bin/ustore_stop.sh

# remove data
ssh_options="-oStrictHostKeyChecking=no \
             -oUserKnownHostsFile=/dev/null \
             -oLogLevel=quiet"

# clean ustore data
hosts=`cat $USTORE_CONF_HOST_FILE | cut -d ':' -f 1`
for i in ${hosts[@]}; do
  echo Clean ustore @ $i ...
  if [ $i == localhost ]; then
    rm ./ustore_data/ustore*
  else
    ssh $ssh_options $i rm $USTORE_HOME/ustore_data/ustore*
  fi
done
echo "----------- All data removed ------------"
