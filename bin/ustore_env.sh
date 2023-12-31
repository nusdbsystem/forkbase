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
USTORE_ENV_DONE=1

# set USTORE_HOME
if [ -z $USTORE_HOME ]; then
  USTORE_HOME=`dirname "${BASH_SOURCE-$0}"`
  USTORE_HOME=`cd "$USTORE_HOME/..">/dev/null; pwd`
fi

# set USTORE_BIN
if [ -z $USTORE_BIN ]; then
  USTORE_BIN="$USTORE_HOME/bin"
fi

# set USTORE_CONF
if [ -z $USTORE_CONF ]; then
  USTORE_CONF="$USTORE_HOME/conf"
fi

# set USTORE_CONF_FILE
if [ -z $USTORE_CONF_FILE ]; then
  USTORE_CONF_FILE="$USTORE_CONF/config.cfg"
fi

# set USTORE_CONF_HOST_FILE
if [ -z $USTORE_CONF_HOST_FILE ]; then
  USTORE_CONF_HOST_FILE=`grep "worker_file:" $USTORE_CONF_FILE | cut -d '"' -f 2`
fi

# set USTORE_CONF_DATA_DIR
if [ -z $USTORE_CONF_DATA_DIR ]; then
  USTORE_CONF_DATA_DIR=`grep "data_dir:" $USTORE_CONF_FILE | cut -d '"' -f 2`
fi

# set USTORE_LOG
if [ -z $USTORE_LOG ]; then
  USTORE_LOG="$USTORE_HOME/log"
fi

# check command existence
[ "$(command -v killall)" ] || echo "WARNING: killall command not found"

# function for killing process
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
