// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "utils/sock.h"

/*
 * SocketServer
 */
SocketServer::SocketServer() {
}

bool SocketServer::svr_init(int port) {
  socket_port = port;
  int yes = 1;
  struct epoll_event event;
  struct sockaddr_in server_addr;

  socket_listen = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_listen < 0) {
    printf("socket error.\n");
    return false;
  }

  if (setsockopt(socket_listen, SOL_SOCKET,
    SO_REUSEADDR, &yes, sizeof(int))) {
    printf("setsockopt failed.\n");
    return false;
  }

  // set non-blocking
  set_nonblock(socket_listen);

  // setup socket address
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(socket_port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  // bind
  if (bind(socket_listen, (struct sockaddr*)&server_addr,
    sizeof(server_addr)) < 0) {
    printf("bind error.\n");
    return false;
  }

  // listen
  if (listen(socket_listen, 5) < 0) {
    printf("listen error.\n");
    return false;
  }

  // init epoll
  epollfd = epoll_create(MAX_EVENTS);
  if (epollfd == -1) {
    printf("epoll create failed.\n");
    return false;
  }
  event.events = EPOLLIN|EPOLLET;
  event.data.fd = socket_listen;

  // epoll control
  if (epoll_ctl(epollfd, EPOLL_CTL_ADD,
    socket_listen, &event) < 0) {
    printf("epoll add fail : df = %d\n", socket_listen);
    return false;
  }

  printf("socket server created with port: %d\n", port);

  return true;
}

int SocketServer::wait_events() {
  int ret;

  while (true) {
    ret = epoll_wait(epollfd, event_list, MAX_EVENTS, TIME_OUT);

    if (ret < 0) {
      if (errno == EINTR && epollfd > 0) {
        usleep(1*1000);
        continue;
      }
      printf("epoll wait failed.\n");
      break;
    } else if (ret == 0) {
      printf("no socket ready for read within %d secs.\n", TIME_OUT/1000);
      sleep(1);
      continue;
    } else {
      break;
    }
  }

  // printf("find %d new events!\n", ret);

  return ret;
}

void SocketServer::accept_conn() {
  int confd;
  struct epoll_event event;
  char client_ip_str[INET_ADDRSTRLEN];
  struct sockaddr_in sin;
  socklen_t len = sizeof(struct sockaddr_in);

  bzero(&sin, len);
  confd = accept(socket_listen, (struct sockaddr*)&sin, &len);

  if (confd == -1) {
    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
      return;
    } else {
      printf("accept failed.\n");
      exit(EXIT_FAILURE);
    }
  }
  if (!inet_ntop(AF_INET, &(sin.sin_addr),
    client_ip_str, sizeof(client_ip_str))) {
    printf("inet_ntop failed.\n");
    exit(EXIT_FAILURE);
  }

  char hostname[24];
  getnameinfo((struct sockaddr*)&sin, len, hostname,
    sizeof(hostname), NULL, 0, 0);

  fd2host[confd] = string(hostname);
  printf("accept a client from: %s(%s), client=%d\n",
    hostname, client_ip_str, confd);

  // set non-block
  set_nonblock(confd);

  // add into epoll
  event.data.fd = confd;
  event.events = EPOLLIN|EPOLLET;
  if (epoll_ctl(epollfd, EPOLL_CTL_ADD,
    confd, &event) == -1) {
    printf("epoll_ctl(EPOLL_CTL_ADD) failed.\n");
    exit(EXIT_FAILURE);
  }
}

int SocketServer::recv_data(int fd, char* buf, int len) {
  int ret;

  memset(buf, 0, len);
  ret = recv(fd, buf, len, 0);

  printf("from client %d,receive %d bytes\n", fd, ret);

  return ret;
}

void SocketServer::discard_conn(int fd) {
  close(fd);
  printf("client=%d closed\n", fd);
}

void SocketServer::svr_close() {
  close(epollfd);
  close(socket_listen);
}

string SocketServer::get_host_by_fd(int fd) {
  return fd2host[fd];
}

void SocketServer::set_nonblock(int sock) {
  int flags;

  flags = fcntl(sock, F_GETFL, 0);
  if (flags <0) {
    printf("fcntl(F_GETFL) failed.\n");
    exit(EXIT_FAILURE);
  }
  if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
    printf("fcntl(F_SETFL) failed.\n");
    exit(EXIT_FAILURE);
  }
}


/*
 * SocketClient
 */
SocketClient::SocketClient() {
  // do nothing here
}

bool SocketClient::cli_init(const char* name, int port) {
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    printf("error opening socket.\n");
    return false;
  }

  // find server
  server = gethostbyname(name);
  if (server == NULL) {
    printf("no such host: %s\n", name);
    return false;
  }

  // init
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr,
    (char *)&serv_addr.sin_addr.s_addr, server->h_length);
  serv_addr.sin_port = htons(port);

  if (connect(sockfd, (struct sockaddr *) &serv_addr,
    sizeof(serv_addr)) < 0) {
    printf("connect failed.\n");
    return false;
  }

  return true;
}

int SocketClient::send_data(char* buf, int len) {
  int ret = write(sockfd, buf, len);

  if (ret < 0) {
    printf("failed to write to socket.\n");
  }

  return ret;
}

void SocketClient::cli_close() {
  close(sockfd);
}

