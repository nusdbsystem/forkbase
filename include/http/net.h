// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_NET_H_
#define USTORE_USTORE_HTTP_NET_H_

#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <sstream>
#include "http/event.h"
#include "http/settings.h"

using std::string;
using std::stringstream;

namespace ustore {

#define TCP_BACKLOG 511

#define ST_SUCCESS 0
#define ST_ERROR -1
#define ST_CLOSED 1
#define ST_INPROCESS 2

/*
 * Net socket class based on Linux TCP socket
 */
class Socket {
 public:
  explicit Socket(int fd): fd_(fd) {}

  Socket(const string& ip, int port)
      : ip_(ip),
        port_(port) {}

  Socket(const string& ip, int port, int fd)
      : ip_(ip),
        port_(port),
        fd_(fd) {}

  ~Socket() {
    if (fd_ > 0) {
      close(fd_);
    }
  }

  // get the file descriptor of the socket
  inline int GetFD() const noexcept {
    return fd_;
  }

 protected:
  int fd_ = -1;
  string ip_;
  int port_ = 0;
};

class ClientSocket : public Socket {
 public:
  ClientSocket(const string& ip, int port)
      : Socket(ip, port) {
  }
  ClientSocket(const string& ip, int port, int fd)
      : Socket(ip, port, fd) {}

  // connect to the server
  int Connect();

  // send buf[0, size] over the socket
  int Send(const void* buf, int size);

  /*
   * read the data (max = size) from socket and put the data in buf
   * return: size of data received
   */
  int Recv(void* buf, int size = DEFAULT_RECV_SIZE);
  /*
   * read the data (max = size) from socket and put the data in a string and return
   * return: received data as a string
   */
  string Recv(int size = DEFAULT_RECV_SIZE);
  /*
   * read the data (max = size) from socket and write the data to stringstream
   * return: size of data received
   */
  int Recv(stringstream& ss, int size = DEFAULT_RECV_SIZE);
};

class ServerSocket : public Socket {
 public:
  explicit ServerSocket(int port, const string& bind_addr = "", int backlog =
                            TCP_BACKLOG)
      : Socket(bind_addr, port),
        backlog_(backlog) {
  }

  /*
   * start listen to the socket
   * return:
   * if failed, return ST_ERROR
   * otherwise, return ST_SUCCESS
   */
  int Listen();

  /*
   * accept a client socket
   * return:
   * if failed, return nullptr
   * otherwise, return the newly created ClientSocket
   */
  ClientSocket* Accept();

 private:
  int backlog_;
};
}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_NET_H_
