// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <stdlib.h>
#include <unistd.h>
#include <thread>

#include "utils/sock.h"
#include "utils/regist.h"
#include "utils/http.h"
#include "utils/config.h"
#include "./daemon.h"

PerfmonDaemon::PerfmonDaemon(int monitor_port, int ui_port) {
  sock_port = monitor_port;
  http_port = ui_port;
}

PerfmonDaemon::~PerfmonDaemon() {
  socket.svr_close();
}

void PerfmonDaemon::start() {
  // start http service first
  std::thread httpThread(startHttpService, http_port, &buffer);

  // start socket service
  while (!socket.svr_init(sock_port)) {
    fprintf(stderr,
      "cannot create socket server at port %d\n, retry later...",
      sock_port);
    usleep(2000000);
  }

  // start accept client reports
  while (true) {
    int cnt = socket.wait_events();
    if (cnt < 0) break;

    // go through all events
    for (int i = 0; i < cnt; ++i) {
      // check epoll error
      if ((socket.event_list[i].events & EPOLLERR) ||
          (socket.event_list[i].events & EPOLLHUP) ||
          !(socket.event_list[i].events & EPOLLIN)) {
        printf("epoll error.\n");
        close(socket.event_list[i].data.fd);
        return;
      }

      // accept new connection
      if (socket.event_list[i].data.fd == socket.socket_listen) {
        socket.accept_conn();
      } else {  // read from socket
        int fd = socket.event_list[i].data.fd;
        int len = socket.recv_data(fd, buf, BUF_LEN);
        // printf("buffer filled with %d bytes\n", len);
        processMessage(socket.get_host_by_fd(fd).c_str(), len);
        // close if find empty message
        if (len == 0) socket.discard_conn(socket.event_list[i].data.fd);
      }
    }
  }
}

void PerfmonDaemon::processMessage(const char* hostname, int len) {
  if (len % UNIT != 0) {
    fprintf(stderr,
      "cannot process damaged message with length %d (unit length = %d)\n",
      len, UNIT);
    return;
  }

  // print message
  /*
  int size = len/UNIT;
  struct ProcInfo *p = (struct ProcInfo *)buf;
  for (int i = 0; i < size; ++i) {
    printf("name=%s cpu=%.1f%% mem(v/r)=%lu/%lu io(r/w)=%d/%d net(s/r)=%d/%d\n",p->name,p->cpu,p->mem_v,p->mem_r,p->io_read,p->io_write,p->net_send, p->net_recv);
    p++;
  }
  */

  // add message into buffer
  buffer.put_back(hostname, buf, len);

  // print messages in buffer
  /*
  ProtoBufferNode* ptr = buffer.get_front();
  while (ptr != nullptr) {
    printf("node = %s t = %ld size = %d\n", ptr->node, ptr->t_monitor, ptr->size);
    ProcInfo *p = ptr->head;
    for (int i = 0; i < ptr->size; ++i) {
    printf("name=%s cpu=%.1f%% mem(v/r)=%lu/%lu io(r/w)=%d/%d net(s/r)=%d/%d\n",p[i].name,p[i].cpu,p[i].mem_v,p[i].mem_r,p[i].io_read,p[i].io_write,p[i].net_send, p[i].net_recv);
    }
    ptr = ptr->next;
  }
  */
}

int main(int argc, char*argv[]) {
//  if (argc < 3) {
//    printf("%s usage: [monitor port] [ui port]\n", argv[0]);
//    exit(0);
//  }

  Config conf;
  conf.loadConfigFile("conf/perfmon.cfg");

  string http_port = conf.get("http_port");
  string sock_port = conf.get("sock_port");
  if (http_port == "" || sock_port == "") {
    std::cerr << "please set sock_host and sock_port in configure file\n";
    return EXIT_SUCCESS;
  }

  string pid_dir = conf.get("pid_dir");
  if (pid_dir != "") setRegistPath(pid_dir.c_str());

  registInPerfmon("perf_daemon");

  printf("starting perfmon daemon...\n");
  PerfmonDaemon *daemon
    = new PerfmonDaemon(atoi(sock_port.c_str()),
    atoi(http_port.c_str()));
  daemon->start();

  printf("closing perfmon daemon...\n");
  delete daemon;

  return 0;
}

