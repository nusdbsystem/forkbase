## dependencies

  * libpcap
  * dbus
  * cpp-netlib
  * gcc with c++11 support 
  * python (to run webserver)

## install

  * run 'make'

## configuration

  * conf/perfmon.cfg: port, hostname, pid file path, web server port, etc.
  * script/slaves: slave nodes where the UStore processes are running
  * webui/js/monitor.js Monitor.API=[pm_daemon hostname]

## run by script

1. start all the backend processes (pm_daemon + pm_monitor)
  
    $ ./script/start_all.sh

2. stop all the backend processes
  
    $ ./script/stop_all.sh

## manual run

### data preparation
  
  * a dir containing multiple pid files named by the process name
  * each pid file contains at least one line, and each line of a pid file is the
    pid of a monitored process 

### setup

1. start script to generate pid files in the folder configured in conf/perfmon.cfg 

    $ ./scripts/ps.sh

2. start monitor master and http daemon at one node
    
    $ ./build/pm_daemon 

3. start monitor program in each node
    
    $ ./build/pm_monitor 

4. check http server in browser
    
    http://monitor_master:http_port

Optional (run web UI):

5. start web server
  
    $ ./scripts/start_webserver.sh

6. check web ui in browser

    http://monitor_master:webserver_port

