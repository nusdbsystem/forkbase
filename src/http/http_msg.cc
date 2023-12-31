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

void Request::PreparePayload() {
  // append parameters to target in the form: /target?k1=v1&k2=v2&...
  int cnt = 0;
  std::string param_str;
  for (auto const& entry : param_) {
    param_str += (cnt++ ? "&" : "?") + entry.first + "=" + entry.second;
  }
  req_.target(target_ + param_str);
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
