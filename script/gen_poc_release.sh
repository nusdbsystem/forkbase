#!/usr/bin/env bash

build_dir=./build
lib_archive=./deps/deps-suse.tar.gz

echo "rm -r $build_dir/poc_release"
rm -r $build_dir/poc_release
echo "mkdir -p $build_dir/poc_release"
mkdir -p $build_dir/poc_release
echo "mkdir -p $build_dir/poc_release/bin"
mkdir -p $build_dir/poc_release/bin
echo "mkdir -p $build_dir/poc_release/lib"
mkdir -p $build_dir/poc_release/lib-suse
echo "$build_dir/script/update_libs.sh $build_dir/bin/test_ustore $build_dir/poc_release/lib"
$build_dir/script/update_libs.sh $build_dir/bin/test_ustore $build_dir/poc_release/lib
echo "cp $build_dir/poc_release/lib/* $build_dir/poc_release/lib-suse"
cp $build_dir/poc_release/lib/* $build_dir/poc_release/lib-suse
echo "tar zxf $lib_archive -C $build_dir/poc_release"
tar zxf $lib_archive -C $build_dir/poc_release
echo "cp -f $build_dir/bin/* $build_dir/poc_release/bin/"
cp -f $build_dir/bin/* $build_dir/poc_release/bin/
#echo "cp -f $build_dir/lib/libustore.so $build_dir/poc_release/lib"
#cp -f $build_dir/lib/libustore.so $build_dir/poc_release/lib
#echo "cp -f $build_dir/lib/libustore.so $build_dir/poc_release/lib-suse"
#cp -f $build_dir/lib/libustore.so $build_dir/poc_release/lib-suse
echo "cp -r $build_dir/conf $build_dir/poc_release/conf"
cp -r $build_dir/conf $build_dir/poc_release/conf
echo "cp -r $build_dir/../perfmon $build_dir/poc_release/perfmon"
cp -r $build_dir/../perfmon $build_dir/poc_release/perfmon
echo "cp -r $build_dir/../doc $build_dir/poc_release/doc"
cp -r $build_dir/../doc $build_dir/poc_release/doc
echo "cp -r $build_dir/../webui $build_dir/poc_release/webui"
cp -r $build_dir/../webui $build_dir/poc_release/webui
echo "tar cxf poc_release.tar.gz $build_dir/poc_release"
tar zcf poc_release.tar.gz $build_dir/poc_release
echo "mv $build_dir/poc_release/ ."
mv $build_dir/poc_release/ .
