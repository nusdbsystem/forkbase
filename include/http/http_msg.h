// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_HTTP_MSG_H_
#define USTORE_HTTP_HTTP_MSG_H_

#include <map>
#include <string>
#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>

namespace ustore {
namespace http {

namespace beast = boost::beast::http;
#define DEFAULT_HTTP_VERSION 11

enum class Verb {
 kGet = 0,
 kPut = 1,
 kPost = 2
};

enum class Format {
 plain = 0,
 json = 1
};

class Request {
 public:
  Request() { SetDefaultFields(); }
  ~Request() = default;

  // Set target and http method
  void SetTargetNMethod(const std::string& target, const Verb method);

  // add customized header fields
  inline void SetHeaderField(const std::string& fld, const std::string& val)
      { req_.set(fld, val); }
  
  // set data to body
  void SetBody(const std::string& data, const Format format);

  // getter
  inline beast::request<beast::string_body> GetReq() { return req_; }
  
 private:
  // Data member
  beast::request<beast::string_body> req_;
  
  // add default header fields
  // called by constructor
  void SetDefaultFields();
};

class Response {
 public:
  Response() = default;
  ~Response() = default;

  const std::map<std::string, std::string> headers();
  
  const std::string& body();
  
  inline beast::response<beast::string_body>& GetRes()
      { return res_; }
  
  inline boost::beast::flat_buffer& GetBuffer() { return buffer_; }
 
 private:
  beast::response<beast::string_body> res_;
  boost::beast::flat_buffer buffer_;
};

}  // namespace http
}  // namespace ustore

#endif  // USTORE_HTTP_HTTP_MSG_H_
