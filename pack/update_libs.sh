#! /usr/bin/env bash
#
# export depedent libs from an executable into a folder
#

if [ $# -ge 2 ]; then
    bin=$1
    lib_dir=$2
else
    echo "Usage: ./update_libs.sh executable lib_dir"
    exit
fi

mkdir -p $lib_dir

for lib in `ldd $bin | cut -d '=' -f2 | cut -d ' ' -f2 | grep so`
do
    echo "Copy $lib to $lib_dir"
    cp $lib $lib_dir
done
