// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_HTTP_MSG_H_
#define USTORE_HTTP_HTTP_MSG_H_

#include <map>
#include <string>

namespace ustore {
namespace http {

enum class Verb {
 kGet = 0,
 kPut = 1,
 kPost = 2
};

class Request {
 public:
  Request() = default;
  ~Request() = default;

  // add customized header fields
  void SetHeaderField(const std::string& fld, const std::string& val);
  // add request parameters (for PUT, POST verbs)
  void AddParameter(const std::string& key, const std::string& val);

 private:
  // add default header fields
  // called by constructor
  void SetDefaultFields();
};

class Response {
 public:
  Response() = default;
  ~Response() = default;

  const std::map<std::string, std::string>& headers();
  const std::string& body();
};

}  // namespace http
}  // namespace ustore

#endif  // USTORE_HTTP_HTTP_MSG_H_
