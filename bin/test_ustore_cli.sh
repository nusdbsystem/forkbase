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
    USTORE_CLEAN="./bin/ustore_clean.sh $@"
    USTORE_START="./bin/ustore_start.sh $libs $@"
else
    libs=""
    CLI="./bin/ustore_cli $@"
    USTORE_CLEAN="./bin/ustore_clean.sh $@"
    USTORE_START="./bin/ustore_start.sh $libs $@"
fi

UNIT_TEST_SCRIPT="./bin/cli_unit_test.ustore"

#### End of Configurations ####

# EXP_OK=0
# EXP_FAIL=1
# EXP_UNKNOWN=2

RED="\033[1m\033[31m"
GREEN="\033[1m\033[32m"
YELLOW="\033[1m\033[33m"
NE="\033[0m"

# run() {
#     echo -e "${YELLOW}>${NE} $1"
#     ${CLI} $1
#     if [[ $2 -eq ${EXP_OK} ]]; then
#         echo -e "${GREEN}[SUCCESS] ...${NE}"
#     elif [[ $2 -eq ${EXP_FAIL} ]]; then
#         echo -e "${RED}[FAILED] ...${NE}"
#     else
#         echo -e "[${GREEN}SUCCESS${NE}/${RED}FAILED${NE}] ..."
#     fi
#     echo
# }

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

# run "put --key abc --branch master --value Hello" 0
# run "is_head -k abc -b master -v KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 0
# run "is_head -k abc -b master -v AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" 0
# run "is_latest -k abc -v KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 0
# run "is_latest -k abc -v AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" 0
# run "put --key abc --branch side --value ustore" 0
# run "put --key abc -u KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB --value ustore" 0
# run "put --key abc -u AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB --value ustore" 1
# run "get -k abc -b master" 0
# run "get -k abc -b side" 0
# run "get -k nokey -b side" 1
# run "get -k abc -b dont-have" 1
# run "branch -k abc -c master -b dev" 0
# run "branch -k abc -c noexist -b dev2" 1
# run "branch -k abc -u KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB -b dev3" 0
# run "branch -k abc -u QU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB -b dev4" 1
# run "merge -k abc -x merged -b master -c side" 0
# run "merge -k abc -x merged -b master -c no-exist" 1
# run "merge -k nokey -x merged -b master -c no-exist" 1
# run "merge -k abc -x 2merged -b master -u KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 0
# run "merge -k abc -x 3merged -b master -u AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB" 1
# run "merge -k abc -x 3merged -b noexist -u A6OQ2335VNJN6TVA2RLOPIXCUFRAS7IA" 1
# run "merge -k abc -x 3merged -u A6OQ2335VNJN6TVA2RLOPIXCUFRAS7IA -v KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 0
# run "merge -k abc -x 4merged -u AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB -v KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 1
# run "merge -k abc -x 4merged -v AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB -u KU54VWWR6GQS6XFGXTXMLXMU3UGC3FLB" 1
# run "list_branch -k abc" 0
# run "list_branch -k nokey" 0
# run "hEAd -k abc -b master" 0
# run "hEAd -k abc -b nobranch" 1
# run "exists -k abc" 0
# run "exists -k nokey" 0
# run "exists -k abc -b master" 0
# run "exists -k abc -b nobranch" 0
# run "latest -k abc" 0
# run "latest -k nokey" 0
# run "list_key" 0
# run "put --key cde -x value -b main" 0
# run "list_key" 0
# run "delete_branch -k abc -b side" 0
# run "exists -k abc -b side" 0
# run "list_branch -k abc" 0
# run "rename -k abc -c master -b main" 0
# run "list_branch -k abc" 0
# run "rename -k abc -c noexist -b main" 1

# exit $?
