#!/usr/bin/env bash
#
# build ustore docker images
#

# change to root dir
cd `dirname "${BASH_SOURCE-$0}"`/..

echo "#########################"
echo "Build ustore:ubuntu-16.04"
echo "#########################"
docker build -f docker/ubuntu/16.04/Dockerfile --force-rm -t ustore:ubuntu-16.04 .
