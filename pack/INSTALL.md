# UStore Installation Guide

## Set Environment Variables

Add into ``~/.bashrc`` file:
```
export USTORE_HOME={PATH_TO_USTORE_DIR}/build
export PATH=$USTORE_HOME/bin:$PATH
export LD_LIBRARY_PATH=$USTORE_HOME/lib:$LD_LIBRARY_PATH
```

Then update variables:
```
source ~/.bashrc
```

## Modify Configuration Files

All configuration files are contained in ``build/conf``:

* `conifg.fig`: general configuration
* `workers.lst`: the list of ustore workers

For local single-node installation, change ``workers.lst`` to contain a single worker:
```
localhost:50500
```

## Unit Test

You can test whether all modules work on your platform:
```
# note: unit test can only run in $USTORE_HOME folder
cd $USTORE_HOME
./bin/test_ustore
```

## Start Service
```
ustore_start.sh
```

## Interaction

You can use the commandline tool ``ustore_cli`` to interact:

* check aliveness: ``ustore_cli info``
* put to a branch: ``ustore_cli put -k test -b master -x value``
* get from a branch: ``ustore_cli get -k test -b master``
* list all options: ``ustore_cli help``

## Stop Service
```
ustore_stop.sh
```

## Cleanup All Data
```
ustore_clean.sh
```
