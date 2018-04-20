#!/usr/bin/env bash
#
# pack a release binary
#

# get environment variables
. `dirname "${BASH_SOURCE-$0}"`/../bin/ustore_env.sh
[ $USTORE_HOME ] || exit 0
cd $USTORE_HOME

# config file paths
build_dir=$USTORE_HOME/build
lib_archive=$USTORE_HOME/deps/deps-suse.tar.gz
release_dir=$USTORE_HOME/ustore_release
release_tar=$USTORE_HOME/ustore_release.tar.gz

# print all commands
# set -x #echo on

# init release dir
echo "Init release dir $release_dir ..."
rm -r $release_dir
mkdir -p $release_dir/build

# export dependent libs
echo "Pack dependent libs ..."
mkdir -p $release_dir/lib
./pack/update_libs.sh $build_dir/bin/test_ustore $release_dir/lib
# remove system libs
rm $release_dir/lib/libc.so.*
rm $release_dir/lib/libm.so.*
rm $release_dir/lib/librt.so.*
rm $release_dir/lib/libz.so.*
rm $release_dir/lib/libstdc++.so.*
rm $release_dir/lib/libpthread.so.*
echo "Pack dependent libs for SUSE ..."
cp -r $release_dir/lib $release_dir/lib-suse
# extract suse libs from archive
tar zxf $lib_archive -C $release_dir

# copy release files
echo "Pack binaries ..."
cp -r $build_dir/bin $release_dir/build
echo "Pack configs ..."
cp -r $build_dir/conf $release_dir/build
# echo "Pack perfmon ..."
# cp -r $USTORE_HOME/perfmon $release_dir
echo "Pack webui ..."
cp -r $USTORE_HOME/webui $release_dir
echo "Pack docs ..."
cp -r $USTORE_HOME/doc $release_dir

# add commit log info
echo "Pack version info ..."
if [ "$(command -v git)" ]; then
  echo "[Latest Commit]" > $release_dir/VERSION
  git log -1 >> $release_dir/VERSION
  echo -e "\n[Commit Log]" >> $release_dir/VERSION
  git log --oneline -20 >> $release_dir/VERSION
else
  echo "Not in a git repository"
fi

# create tarball
echo "Create tarball ..."
tar zcf $release_tar -C $USTORE_HOME ustore_release
echo "Released: $release_tar"
