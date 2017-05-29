// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_SERVER_H_
#define USTORE_USTORE_HTTP_SERVER_H_

#include <thread>
#include <string>
#include "http/net.h"

namespace ustore {

/*
 * Http Server class
 */
class HttpServer {
 public:
  explicit HttpServer(int port,
                      const string& bind_addr = "", int backlog = TCP_BACKLOG);
  ~HttpServer();

  /*
   * start the server
   * @threads_num:  number of threads the server have
   *                one thread for one event loop
   */
  int Start(int threads_num = 1);

  // stop the server
  inline void Stop() {
    for (int i = 0; i < threads_num_; i++) el_[i]->Stop();
  }

  // return the ServerSocket reference
  inline ServerSocket& GetServerSocket() {
    return ss_;
  }

  // set the maximum size of the event loop
  // i.e., the max number of file descriptors the event loop supports
  inline void SetEventLoopSize(int size) {
    el_size_ = size;
  }

  // dispatch the Client to an eventloop
  int DispatchClientSocket(ClientSocket* cs);

 private:
  ServerSocket ss_;
  int threads_num_ = 1;
  int el_size_ = 10000;
  std::thread** ethreads_ = nullptr;
  EventLoop** el_ = nullptr;
};
}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_SERVER_H_
