// Copyright (c) 2017 The Ustore Authors.

#include "http/http_msg.h"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include "utils/logging.h"

namespace ustore {
namespace http {

void Request::SetMethod(Verb method) {
  switch (method) {
    case Verb::kGet:
      req_.method(beast::verb::get);
      break;
    case Verb::kPut:
      req_.method(beast::verb::put);
      break;
    case Verb::kPost:
      req_.method(beast::verb::post);
      break;
    default:
      LOG(FATAL) << "Unsupported http verb";
  }
}

void Request::SetBody(const std::string& data, Format content_type) {
  switch (content_type) {
    case Format::kPlain:
      req_.set(beast::field::content_type, "text/plain");
      break;
    case Format::kJson:
      req_.set(beast::field::content_type, "application/json");
      break;
  }
  req_.body() = data;
}

void Request::PreparePayload() {
  if (!param_.empty()) {
    bool first = true;
    for (auto const& entry : param_) {
      if (!first) {
        target_ += "&";
      } else {
        target_ += "?";
        first = false;
      }
      target_ += entry.first + "=" + entry.second;
    }
  }
  req_.target(target_);
  req_.prepare_payload();
}

void Request::SetDefaultFields() {
  req_.version(kDefaultHttpVersion);
  req_.set(beast::field::user_agent, BOOST_BEAST_VERSION_STRING);
}

const std::map<std::string, std::string>& Response::headers() {
  if (headers_.empty()) {
    for (auto const& field : res_) {
      headers_[std::string(field.name_string())] = std::string(field.value());
    }
  }
  return headers_;
}

}  // namespace http
}  // namespace ustore
