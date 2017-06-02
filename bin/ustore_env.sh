#!/usr/bin/env bash
#
# set ustore environment variables, includes:
#   * USTORE_HOME
#   * USTORE_BIN
#   * USTORE_CONF
#   * USTORE_LOG
#

# exit if varaiables already set
[ -z $USTORE_ENV_DONE ] || exit 0

# set USTORE_BIN
if [ -z $USTORE_BIN ]; then
  USTORE_BIN=`dirname "${BASH_SOURCE-$0}"`
  USTORE_BIN=`cd "$USTORE_BIN">/dev/null; pwd`
fi

# set USTORE_HOME
if [ -z $USTORE_HOME ]; then
  USTORE_HOME=`cd "$USTORE_BIN/..">/dev/null; pwd`
fi

# set USTORE_CONF
if [ -z $USTORE_CONF ]; then
  USTORE_CONF=$USTORE_HOME/conf
fi

# set USTORE_LOG
if [ -z $USTORE_LOG ]; then
  USTORE_LOG=$USTORE_HOME/log
fi

wait_and_fkill() {
    fkill=$1
    pid=$2
    stime=$3
    # echo "wait for $pid for $stime and then $fkill"

    counter=0
    while [ true ];
    do
        kill -0 $pid > /dev/null 2>&1
        status=$?
        if [ $status = 0 ]; then
            sleep 1
            counter=$((counter+1))
            if [ $counter = $stime ]; then
                # echo "force kill the process"
                $fkill > /dev/null 2>&1
            fi
        else
           return 
        fi
    done
}


# mark that we have done all
USTORE_ENV_DONE=1
