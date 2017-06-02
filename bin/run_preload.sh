#! /usr/bin/env bash

if [ $# -ge 2 ]; then
    ustore_lib_path=$1
    bin=$2
else
    echo "Usage: ./run_preload.sh ustore_lib_path executable [args]"
    exit
fi

# ustore_lib_path=ustorelib
pre_libs=""

for lib in `ls $ustore_lib_path`
do
    pre_libs="$pre_libs $ustore_lib_path/$lib"
done

shift
shift
echo "LD_PRELOAD=$pre_libs $bin $@"
LD_PRELOAD="$pre_libs" $bin $@ 
