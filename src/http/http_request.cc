// Copyright (c) 2017 The Ustore Authors.

#include <string.h>
#include <algorithm>
#include <cassert>
#include "utils/utils.h"
#include "http/http_request.h"

namespace ustore {

const unordered_map<string, CommandType> HttpRequest::cmddict_ = {
    {"/get", GET},
    {"/put", PUT},
    {"/merge", MERGE},
    {"/branch", BRANCH},
    {"/rename", RENAME},
    {"/list", LIST},
    {"/head", HEAD},
    {"/latest", LATEST}
};

int HttpRequest::ParseFirstLine(char* buf, int start, int end) {
  int pos = start;
  int is, ie;  // start and end position of each item

  // method
  TrimSpace(buf, pos, end);
  if (unlikely(pos > end)) return ST_ERROR;
  is = ie = pos;
  ie = FindCharToLower(buf, is, end, ' ');
  CHECK(ie > is);
  method_ = string(buf, is, ie-is);

  // uri
  pos = ie+1;
  TrimSpace(buf, pos, end);
  if (unlikely(pos > end)) return ST_ERROR;
  is = ie = pos;
  ie = FindChar(buf, is, end, ' ');
  CHECK(ie > is);
  uri_ = string(buf, is, ie-is);

  // http_version_
  pos = ie+1;
  TrimSpace(buf, pos, end);
  if (unlikely(pos > end)) return ST_ERROR;
  http_version_ = string(buf, pos, end-pos+1);

  return ST_SUCCESS;
}

int HttpRequest::ParseOneLine(char* buf, int start, int end) {
  TrimSpace(buf, start, end);
  if (unlikely(start > end)) return ST_ERROR;

  int is = start, ie = FindChar(buf, start, end, ':');
  if (ie < end) ToLower(buf, is, ie-1);
  string key = string(buf, is, ie-is);
  ie++;
  TrimSpace(buf, ie, end);
  if (unlikely(ie > end)) {
    if (headers_.count("content-length")) {
      int cl = atoi(headers_["content-length"].c_str());
      while (key.length() > cl) {
        LOG(WARNING) << "Content larger than the specified content-length";
        key.pop_back();
      }
    }
    // LOG(WARNING) << "found the parameter: " << key;
    headers_[kParaKey] = key;
  } else {
    TrimSpecialReverse(buf, end, ie);
    headers_[key] = string(buf, ie, end-ie+1);
    // LOG(WARNING) << "Parsed: " << key << " : " << headers_[key];
  }
  return ST_SUCCESS;
}

unordered_map<string, string> HttpRequest::ParseParameters() {
  unordered_map<string, string> kv;
  if (headers_.count(kParaKey)) {
     string& para = headers_[kParaKey];
     size_t cur = 0, prev = 0;
     while ((cur = para.find('&', cur)) != std::string::npos) {
       // LOG(WARNING) << para.substr(prev, cur-prev);
       int ep = para.find('=', prev);
       CHECK_LT(ep, cur);
       kv[para.substr(prev, ep-prev)] = para.substr(ep+1, cur-ep-1);
       cur++;
       prev = cur;
     }

     int ep = para.find('=', prev);
     CHECK_NE(ep, cur);
     kv[para.substr(prev, ep-prev)] = para.substr(ep+1);
  }
  return kv;
}

int HttpRequest::ReadAndParse(ClientSocket* socket) {
  char buf[MAX_HEADER_SIZE];
  int nread = socket->Recv(buf, MAX_HEADER_SIZE);
  if (unlikely(nread <= 0)) {  // remote has close the socket
     return ST_CLOSED;
  }

  // LOG(WARNING) << "Received: " << buf;

  int ls = 0, le = 0;  // start and end position of each line
  int pos = 0;
  int linenum = 0;
  while (likely(pos < nread)) {
    TrimSpecial(buf, pos, nread-1);
    if (unlikely(pos >= nread)) break;
    ls = le = pos;
    le = FindChar(buf, ls, nread-1, '\n');  // find the line breaker \r\n or \n
    // buf[le-1] == '\n' or le == nread, anyway, buf[ls,le-1] forms a line
    assert(le > ls);
    le--;  // backwards 1 position (le == nread OR buf[le] = '\n')
    pos = le+1;

    TrimSpecialReverse(buf, le, ls);
    if (unlikely(linenum == 0)) {
      if (ParseFirstLine(buf, ls, le) == ST_ERROR) return ST_ERROR;
    } else {
      if (ParseOneLine(buf, ls, le) == ST_ERROR) return ST_ERROR;
    }
    linenum++;
  }

  if (likely(headers_.count("connection"))) {
    string& value = headers_["connection"];
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (likely(value.find("keep-alive") != string::npos)) {
      keep_alive_ = true;
    }
  }

//  LOG(WARNING) << "method = " << method_.c_str() << ", uri = " << uri_.c_str()
//      << ", http_version" << ":" << http_version_.c_str();
//  for (auto& it: headers_) {
//    LOG(WARNING) << it.first.c_str() << ":" << it.second.c_str();
//  }
  return ST_SUCCESS;
}


int HttpRequest::Respond(ClientSocket* socket, const string response) {
  if (method_ != "post") {
    if (unlikely(kBadRequest.length() !=
        socket->Send(kBadRequest.c_str(), kBadRequest.length()))) {
      return ST_ERROR;
    }
    LOG(WARNING) << "unsupported method: " << method_.c_str();
    return ST_SUCCESS;
  }

  char header[MAX_HEADER_SIZE];
  char sbuf[MAX_RESPONSE_SIZE];
  int pos = 0;

  memcpy(header+pos, kHttpVersion.c_str(), kHttpVersion.length());
  pos += kHttpVersion.length();

  memcpy(header+pos, status_.c_str(), status_.length());
  pos += status_.length();

  memcpy(header+pos, kOtherHeaders.c_str(), kOtherHeaders.length());
  pos += kOtherHeaders.length();


  memcpy(header+pos, kContentLen.c_str(), kContentLen.length());
  pos += kContentLen.length();
  // end of header
  pos += sprintf(header+pos, "%ld\r\n\r\n", response.length());

  memcpy(header+pos, response.c_str(), response.length());
  pos += response.length();

  if (unlikely(pos != socket->Send(header, pos))) {
    return ST_ERROR;
  } else {
    return ST_SUCCESS;
  }
}

}  // namespace ustore

