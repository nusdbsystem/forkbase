// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_HTTP_REQUEST_H_
#define USTORE_USTORE_HTTP_HTTP_REQUEST_H_

#include <unordered_map>
#include <iostream>
#include <sstream>
#include <string>
#include "http/net.h"
#include "http/settings.h"

using std::string;
using std::unordered_map;

namespace ustore {

const string CRLF = "\r\n";
const string kContentLen = "Content-Length: ";
const string kHttpVersion = "HTTP/1.1 ";
const string kConnection = "Connection: ";
const string kOk = "202 OK\r\n";
const string kNotFound = "404 Not Found\r\n";
const string kBadRequest = "400 Bad Request\r\n";
// const string kKeepAlive = "Connection: keep-alive\r\n";
// const string kClosed = "Connection: closed\r\n";
const string kContentType = "Content-Type: ";
const string kOtherHeaders =
    "Connection: keep-alive\r\nServer: Simple Http Server\r\n";
const string kParaKey = "para";

enum CommandType {
  GET,
  PUT,
  MERGE,
  BRANCH,
  RENAME,
  LIST,
  HEAD,
  LATEST
};


/*
 * used to parse the http request and prepare the response
 */
class HttpRequest {
 public:
  HttpRequest() {}

  // parse the data read from socket to
  // fill the necessary header fields in this object
  int ReadAndParse(ClientSocket* socket);

  /*
   * based on the header fields
   * get the required resource and respond to the client
   */
  int Respond(ClientSocket* socket, const string response);

  // whether or not close the socket
  inline bool KeepAlive() { return keep_alive_; }

  // get the POST data
  inline unordered_map<string, string> GetParameters() {
    return ParseParameters();
  }

  // get the command
  inline CommandType GetCommand() const {
    CHECK(cmddict_.count(uri_));
    return cmddict_.at(uri_);
  }

 private:
  // parse the parameter list
  unordered_map<string, string> ParseParameters();

  /*
   * parse the first line of the header
   * e.g., Get / HTTP/1.1
   * line: buf[start, end] excluding \r\n
   */
  int ParseFirstLine(char* buf, int start, int end);

  /*
   * parse the one line of the header (except the first line)
   * e.g., Connection: keep-alive
   * line: buf[start, end] excluding \r\n
   * line: buf[start, end] excluding \r\n
   */
  int ParseOneLine(char* buf, int start, int end);

  /*
   * trim the whitespace character in buf[start, end]
   * from buf[start] onwards and move the start position
   */
  inline void TrimSpace(const char* buf, int& start, int end) {
    while (start <= end && buf[start] == ' ') start++;
  }

  /*
   * trim the special character (e.g., \r, \n, ' ') in buf[start, end]
   * from buf[start] onwards and move the start position
   */
  inline void TrimSpecial(const char* buf, int& start, int end) {
    while (start <= end && (buf[start] == ' '
        || buf[start] == '\n' || buf[start] == '\r')) start++;
  }

  /*
   * trim the special character (e.g., \r, \n, ' ') in buf[start, end]
   * from buf[end] backwards and move the end position
   */
  inline void TrimSpecialReverse(const char* buf, int& end, int start) {
    while (end >= start && (buf[end] == ' '
        || buf[end] == '\n' || buf[end] == '\r')) end--;
  }

  /*
   * return the first position where it is character c
   * within buf[start, end]
   * if not found, return end+1
   * find within [start, end]
   */
  inline int FindChar(const char* buf, int start, int end, char c) {
    while (start <= end) {
      if (buf[start] == c) break;
      start++;
    }
    return start;
  }

  inline void ToLower(char* buf, int start, int end) {
    while (start <= end) {
      buf[start] = std::tolower(buf[start]);
      start++;
    }
  }

  /*
   * find the position of the specified character within [start, end]
   * and tolower the character before this character
   */
  inline int FindCharToLower(char* buf, int start, int end, char c) {
    while (start <= end) {
      if (buf[start] == c) {
        break;
      } else {
        buf[start] = std::tolower(buf[start]);
      }
      start++;
    }
    return start;
  }

  string method_;
  string uri_;
  string http_version_;
  bool keep_alive_ = false;
  unordered_map<string, string> headers_;
  static const unordered_map<string, CommandType> cmddict_;

  // status_: e.g. 202 OK, 404 Not Found
  string status_ = kOk;
};
}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_HTTP_REQUEST_H_
