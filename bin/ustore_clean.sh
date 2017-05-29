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
rm ./ustore_data/ustore*
echo "----------- All data removed ------------"
