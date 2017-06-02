#!/usr/bin/env bash

# get environment variables
. $(dirname "${BASH_SOURCE-$0}")/ustore_env.sh

# go to ustore home to execute binary
cd ${USTORE_HOME}

set -o nounset
set -o pipefail

#### Begin of Configurations ####
CLI="./bin/ustore_cli"
#### End of Configurations ####

EXP_OK=0
EXP_FAIL=1
EXP_UNKNOWN=2

RED="\033[1m\033[31m"
GREEN="\033[1m\033[32m"
YELLOW="\033[1m\033[33m"
NE="\033[0m"

run() {
    echo -e "${YELLOW}>${NE} $1"
    ${CLI} $1
    if [[ $2 -eq ${EXP_OK} ]]; then
        echo -e "${GREEN}[SUCCESS] ...${NE}"
    elif [[ $2 -eq ${EXP_FAIL} ]]; then
        echo -e "${RED}[FAILED] ...${NE}"
    else
        echo -e "[${GREEN}SUCCESS${NE}/${RED}FAILED${NE}] ..."
    fi
    echo
}

run "put --key abc --branch master --value Hello" 0
run "get -k abc -b master" 2
# run "branch -k abc --ref-branch master -b dev"
run "list_branch -k abc" 1
# run "hEAd -k abc -b master"

exit $?
