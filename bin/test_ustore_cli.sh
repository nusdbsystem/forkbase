#!/usr/bin/env bash

# get environment variables
. $(dirname "${BASH_SOURCE-$0}")/ustore_env.sh

# go to ustore home to execute binary
cd ${USTORE_HOME}

set -o nounset
set -o pipefail

#### Begin of Configurations ####

if [ $# -ge 1 ]; then
    libs=$1
    echo "Using preload $libs"
    shift
    CLI="./bin/run_preload.sh $libs bin/ustore_cli $@"
else
    libs=""
    CLI="./bin/ustore_cli $@"
fi

USTORE_CLEAN="./bin/ustore_clean.sh $@"
USTORE_START="./bin/ustore_start.sh $libs $@"

UNIT_TEST_SCRIPT="./bin/cli_unit_test.ustore"

#### End of Configurations ####

RED="\033[1m\033[31m"
GREEN="\033[1m\033[32m"
YELLOW="\033[1m\033[33m"
NE="\033[0m"

${USTORE_CLEAN} && ${USTORE_START}

if [[ $? -ne 0 ]]; then
    echo "Failed to start/restart the UStore service"
    exit -1
fi

${CLI} --script ${UNIT_TEST_SCRIPT}
RST=$?

if [[ ${RST} -eq 0 ]]; then
    echo -e "${GREEN}[SUCCESS]${NE} All tests have passed."
else
    echo -e "${RED}[FAILED]${NE} Failed test is found!"
fi

exit $?
