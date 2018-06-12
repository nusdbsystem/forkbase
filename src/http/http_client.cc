// Copyright (c) 2017 The Ustore Authors.

#include "http/http_client.h"

#include <boost/asio/connect.hpp>
#include "utils/logging.h"

namespace ustore {
namespace http {

using tcp = boost::asio::ip::tcp;

bool HttpClient::Connect(const std::string& host, const std::string& port) {
  try {
    host_ = host;
    tcp::resolver resolver{io_context_};
    auto results = resolver.resolve(host, port);
    boost::asio::connect(socket_, results.begin(), results.end());
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

bool HttpClient::Send(Request* request) {
  try {
    // initialize payload
    request->req_.prepare_payload();
    beast::write(socket_, request->req_);
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

bool HttpClient::Receive(Response* response) {
  try {
    // Receive the HTTP response
    beast::read(socket_, response->buffer_, response->res_);
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

bool HttpClient::Shutdown() {
  try {
    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    // report not_connected if happens
    if (ec && ec != boost::system::errc::not_connected)
      throw boost::system::system_error{ec};
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

}  // namespace http
}  // namespace ustore
