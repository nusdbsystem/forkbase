#!/usr/bin/env bash
#
# stop worker services
#

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/ustore_env.sh
cd $USTORE_HOME

ustore_kill="killall -q -w -s SIGTERM -r ustored"
ustore_fkill="killall -q -s SIGKILL -r ustored"

$ustore_kill > /dev/null 2>&1 &
kpid=$!
wait_and_fkill "$ustore_fkill" $kpid 20
