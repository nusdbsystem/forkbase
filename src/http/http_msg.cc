// Copyright (c) 2017 The Ustore Authors.

#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/version.hpp>
#include "http/http_msg.h"
#include "utils/logging.h"

namespace ustore {
namespace http{

namespace beast = boost::beast::http;

void Request::SetTargetNMethod(const std::string& target, const Verb method) {
  req_.target(target);
  switch(method) {
    case Verb::kGet: {
      req_.method(beast::verb::get);
      break;
    }
    case Verb::kPut: {
      req_.method(beast::verb::put);
      break;
    }
    case Verb::kPost: {
      req_.method(beast::verb::post);
      break;
    }
    default: {
      assert(false);
    }
  }
}


void Request::SetBody(const std::string& data, const Format content_type) {
  switch(content_type) {
    case Format::plain: {
      req_.set(beast::field::content_type, "text/plain");
      break;
    }
    case Format::json: {
      req_.set(beast::field::content_type, "application/json");
      break;
    }
  }
  req_.body() = data;
  req_.prepare_payload();
}

void Request::SetDefaultFields() {
  req_.version(DEFAULT_HTTP_VERSION);
  req_.set(beast::field::user_agent, BOOST_BEAST_VERSION_STRING);
}

const std::string& Response::body() {
  return res_.body();
}

const std::map<std::string, std::string> Response::headers() {
  std::map<std::string, std::string> headers;
  for (auto const& field : res_) {
    headers.insert({to_string(field.name()).data(), field.value().data()});
  }
  return headers;
}

} // namespace http
} // namespace ustore
