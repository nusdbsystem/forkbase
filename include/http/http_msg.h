// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_HTTP_MSG_H_
#define USTORE_HTTP_HTTP_MSG_H_

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <map>
#include <string>

namespace ustore {
namespace http {

namespace beast = boost::beast::http;

enum class Verb {
  kGet = 0,
  kPut = 1,
  kPost = 2
};

enum class Format {
  kPlain = 0,
  kJson = 1
};

class Request {
  friend class HttpClient;

 public:
  Request() {
    SetDefaultFields();
  }
  Request(const std::string& target, Verb method) : Request() {
    SetTarget(target);
    SetMethod(method);
  }
  ~Request() = default;

  // Set target
  inline void SetTarget(const std::string& target) { target_ = target; }
  // Set method verb
  void SetMethod(Verb method);
  // Add customized header fields
  inline void SetHeaderField(const std::string& fld, const std::string& val) {
    req_.set(fld, val);
  }
  inline void AddParameter(const std::string& key, const std::string& val) {
    param_.insert({key, val});
  }
  // Set data body
  void SetBody(const std::string& data, Format format);
  // Prepare payload
  void PreparePayload();

 private:
  static constexpr int kDefaultHttpVersion = 11;

  // add default header fields
  // called by constructor
  void SetDefaultFields();

  beast::request<beast::string_body> req_;
  std::string target_;
  std::map<std::string, std::string> param_;
};

class Response {
  friend class HttpClient;

 public:
  Response() = default;
  ~Response() = default;

  const std::map<std::string, std::string>& headers();
  inline const std::string& body() { return res_.body(); }
  inline int code() { return res_.result_int(); }

 private:
  beast::response<beast::string_body> res_;
  boost::beast::flat_buffer buffer_;
  std::map<std::string, std::string> headers_;  // to hold header strings
};

}  // namespace http
}  // namespace ustore

#endif  // USTORE_HTTP_HTTP_MSG_H_
