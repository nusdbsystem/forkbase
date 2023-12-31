// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_HTTP_CLIENT_H_
#define USTORE_HTTP_HTTP_CLIENT_H_

#include <boost/asio/ip/tcp.hpp>
#include <string>

#include "http/http_msg.h"

namespace ustore {
namespace http {

class HttpClient {
 public:
  HttpClient() = default;
  ~HttpClient() = default;

  /*
   *  Connect to a http server in host:port format
   */
  bool Connect(const std::string& host, const std::string& port);
  /*
   *  Send request of type verb to http://host:port/url
   */
  bool Send(Request* request);
  /*
   *  Get response message from server
   */
  bool Receive(Response* response);
  /*
   *  Shutdown the connection
   */
  bool Shutdown();

 private:
  boost::asio::io_context io_context_;
  boost::asio::ip::tcp::socket socket_{io_context_};
  std::string host_;
};

}  // namespace http
}  // namespace ustore

#endif  // USTORE_HTTP_HTTP_CLIENT_H_
