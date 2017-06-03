#! /usr/bin/env bash

if [ $# -ge 2 ]; then
    bin=$1
    lib_dir=$2
else
    echo "Usage: ./update_libs.sh executable lib_dir"
    exit
fi

mkdir $lib_dir
for lib in `ldd $bin | cut -d '=' -f2 | cut -d ' ' -f2 | grep so`
do
    echo "copy $lib to $lib_dir"
    cp $lib $lib_dir
done
