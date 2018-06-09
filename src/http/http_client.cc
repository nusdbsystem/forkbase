#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/connect.hpp>
#include "http/http_msg.h"
#include "http/http_client.h"
#include "utils/logging.h"

namespace ustore {
namespace http {

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast::http;

bool HttpClient::Connect(const std::string& host, const std::string& port) {
  try{
    host_ = host;
    tcp::resolver resolver{io_context_};

    const auto results = resolver.resolve(host, port);
    boost::asio::connect(socket_, results.begin(), results.end());
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

bool HttpClient::Send(const std::string& target, const Verb verb, Request* request) {
  try {
    request->SetHeaderField("host", host_);
    request->SetTargetNMethod(target, verb);
    beast::write(socket_, request->GetReq());
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}

bool HttpClient::Receive(Response* response) {
  try {
    // Receive the HTTP response
    beast::read(socket_, response->GetBuffer(), response->GetRes());
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
    if(ec && ec != boost::system::errc::not_connected)
      throw boost::system::system_error{ec};
    
    return true;
  } catch(std::exception const& e) {
    LOG(FATAL) << e.what();
    return false;
  }
}
} // namespace http
} // namespace ustore
