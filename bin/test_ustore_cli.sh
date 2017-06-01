#!/bin/bash
set -o nounset
set -o pipefail

#### Begin of Configurations ####
CLI="./bin/ustore_cli"

#### End of Configurations ####

run() {
    echo -e "\033[1m\033[33m>\033[0m $1"
    ${CLI} $1
    echo
}

run "put --key abc --branch master --value Hello"
run "get -k abc -b master"
run "branch -k abc --ref-branch master -b dev"
run "list_branch -k abc"
run "hEAd -k abc -b master"

exit $?
