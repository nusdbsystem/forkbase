## dependencies

  * python
  * django (used in python, install by "pip install Django")
  * subprocess, os, sys (used in python, standard module in Python 2.7, install them manually if not)

## install

  * not required

## configuration

  * set ip and port in run_server.sh

## run by script

  1. start
  ```
    $ ./run_server.sh
  ```

  2. stop
    it sounds ridicules but django doesn't provide any 'stop' function, so that you will have to stop by `command + c` or kill the thread

## manual run

  ```
    $ python ./manage.py runserver $port
  ```

### functions

  1. login
    user needs to input ustore server $ip:$port to connect. e.g., soccf-dbr3-025.d1.comp.nus.edu.sg port:60601.

    *** WARNING:
    The program will not check if the address is available. User should ensure it himself.

    The ustore server address is saved as an instance in the webpage server, which will be lost if the webpage server is restarted. Go to any page to re-enter the address and connect.

  2. list
    Keys are listed.
    Click 'b' or 'v' to list the branches or versions of the key.
    The head version of a branch is shown besides the branch.

    Hover on branch or version to do get/put/rename/branch/merge.

    Click 'merge' and the branch/version will serve as referal, and then click another branch/version to serve as target.

  3. put/get/branch/merge
    simple as you see
