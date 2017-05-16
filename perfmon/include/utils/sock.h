// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_UTILS_SOCK_H_
#define USTORE_PERFMON_UTILS_SOCK_H_

#include <stdlib.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <map>
#include <string>

#define MAX_EVENTS 1000
#define TIME_OUT 5000

using std::string;
using std::map;

class SocketServer{
 public:
  struct epoll_event event_list[MAX_EVENTS];
  int socket_listen;

  SocketServer();

  bool svr_init(int port);
  int wait_events();
  void accept_conn();
  int recv_data(int fd, char* buf, int len);
  void discard_conn(int fd);
  void svr_close();
  string get_host_by_fd(int fd);

 private:
  int socket_port;
  int epollfd;
  map<int, string> fd2host;

  void set_nonblock(int sock);
};

class SocketClient{
 public:
  SocketClient();

  bool cli_init(const char* name, int port);
  int send_data(char* buf, int len);
  void cli_close();

 private:
  int sockfd;
  struct sockaddr_in serv_addr;
  struct hostent *server;
};

#endif  // USTORE_PERFMON_UTILS_SOCK_H_

