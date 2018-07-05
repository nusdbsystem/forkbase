#!/usr/bin/env bash
#
# pack a release binary
#

# set environment variables
#   USTORE_ROOT: git repository dir
#   USTORE_HOME: build dir
#   LUCENE_TARGET: lucene target dir

RELEASE_NAME="ustore_release"
RELEASE_DIR="`pwd`/$RELEASE_NAME"
RELEASE_TAR="$RELEASE_DIR".tar.gz

if [ -z $USTORE_ROOT ]; then
  USTORE_ROOT=`dirname "${BASH_SOURCE-$0}"`
  USTORE_ROOT=`cd "$USTORE_ROOT/..">/dev/null; pwd`
fi
if [ -z $USTORE_HOME ]; then
  . `dirname "${BASH_SOURCE-$0}"`/../build/bin/ustore_env.sh
fi
if [ -z $LUCENE_TARGET ]; then
  LUCENE_TARGET="$USTORE_ROOT/lucene/target"
fi
[ $USTORE_ROOT ] && [ $USTORE_HOME ] && [ $LUCENE_TARGET ] || exit 0
cd $USTORE_ROOT

# config file paths
build_dir=$USTORE_HOME
lib_archive="$USTORE_ROOT/deps/deps-suse.tar.gz"
release_root=$RELEASE_DIR
release_build="$release_root/build"

# print all commands
# set -x #echo on

# init release dir
echo "Init release dir $release_root ..."
rm -r $release_root
mkdir -p $release_build

# export dependent libs
echo "Pack dependent libs ..."
mkdir -p $release_build/lib
./pack/update_libs.sh $build_dir/bin/test_ustore $release_build/lib
# remove system libs
rm $release_build/lib/libc.so.*
rm $release_build/lib/libm.so.*
rm $release_build/lib/librt.so.*
rm $release_build/lib/libz.so.*
rm $release_build/lib/libstdc++.so.*
rm $release_build/lib/libpthread.so.*
echo "Pack dependent libs for SUSE ..."
cp -r $release_build/lib $release_build/lib-suse
# extract suse libs from archive
tar zxf $lib_archive -C $release_build

# copy release files
echo "Pack binaries ..."
cp -r $build_dir/bin $release_build
echo "Pack configs ..."
cp -r $build_dir/conf $release_build
# echo "Pack perfmon ..."
# cp -r $USTORE_ROOT/perfmon $release_root
echo "Pack webui ..."
cp -r $USTORE_ROOT/webui $release_root
echo "Pack docs ..."
cp -r $USTORE_ROOT/docs $release_root
cp -r $USTORE_ROOT/pack/INSTALL.md $release_root
echo "Pack docker ..."
cp -r $USTORE_ROOT/docker $release_root
echo "Pack compilation info ..."
cp $build_dir/CMakeCache.txt $release_root
echo "Pack lucene ..."
mkdir $release_root/lucene
cp $USTORE_ROOT/lucene/README.md $release_root/lucene
cp $LUCENE_TARGET/LuceneJAR-0.0.1-SNAPSHOT.jar $release_root/lucene

# add commit log info
echo "Pack version info ..."
if [ "$(command -v git)" ]; then
  echo "[Latest Commit]" > $release_root/VERSION
  git log -1 >> $release_root/VERSION
  echo -e "\n[Commit Log]" >> $release_root/VERSION
  git log --oneline -20 >> $release_root/VERSION
else
  echo "Not in a git repository"
fi

# create tarball
echo "Create tarball ..."
# tar zcf $RELEASE_TAR -C $USTORE_ROOT ustore_release
cd $RELEASE_DIR/..
tar zcf $RELEASE_TAR $RELEASE_NAME
echo "Released: $RELEASE_TAR"
