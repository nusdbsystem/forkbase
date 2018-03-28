// Copyright (c) 2017 The Ustore Authors.

#include <string.h>
#include <algorithm>
#include <cassert>
#include "utils/utils.h"
#include "http/http_request.h"

namespace ustore {

using std::unordered_map;

const unordered_map<string, CommandType> HttpRequest::cmddict_ = {
    {"/get", CommandType::kGet},
    {"/put", CommandType::kPut},
    {"/merge", CommandType::kMerge},
    {"/branch", CommandType::kBranch},
    {"/rename", CommandType::kRename},
    {"/delete", CommandType::kDelete},
    {"/list", CommandType::kList},
    {"/head", CommandType::kHead},
    {"/latest", CommandType::kLatest},
    {"/exists", CommandType::kExists},
    {"/islatestversion", CommandType::kIsLatestVersion},
    {"/isbranchhead", CommandType::kIsBranchHead},
    {"/get-ds", CommandType::kGetDataset}
};

int HttpRequest::ParseFirstLine(char* buf, int start, int end) {
  int pos = start;
  int is, ie;  // start and end position of each item

  DLOG(INFO) << "First Line: " + std::string(buf, start, end-start+1);
  // method
  TrimSpace(buf, pos, end);
  if (unlikely(pos > end)) return ST_ERROR;
  is = ie = pos;
  ie = FindCharToLower(buf, is, end, ' ');
  CHECK(ie > is);
  method_ = string(buf, is, ie-is);

  if (method_ == "post") {
    // uri
    pos = ie + 1;
    TrimSpace(buf, pos, end);
    if (unlikely(pos > end))
      return ST_ERROR;
    is = ie = pos;
    ie = FindChar(buf, is, end, ' ');
    CHECK(ie > is);
    uri_ = string(buf, is, ie - is);
  } else if (method_ == "get") {
    // uri
    pos = ie + 1;
    TrimSpace(buf, pos, end);
    if (unlikely(pos > end))
      return ST_ERROR;
    is = ie = pos;
    ie = FindChar(buf, is, end, ' ');
    CHECK(ie > is);

    int p = FindChar(buf, is, ie, '?');
    CHECK(p > is);
    if (p > ie) {  // no parameter
      uri_ = string(buf, is, ie - is);
    } else {
      uri_ = string(buf, is, p - is);
      headers_[kParaKey] = string(buf, p+1, ie - p - 1);
    }
  } else {
    LOG(WARNING) << "Unsupported method: " << method_;
  }

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

  DLOG(INFO) << "One line: " << string(buf, start, end-start+1);

  int is = start, ie = FindChar(buf, start, end, ':');
  if (ie < end) ToLower(buf, is, ie-1);
  string key = string(buf, is, ie-is);
  ie++;
  TrimSpace(buf, ie, end);
  if (unlikely(ie > end)) {
    if (headers_.count("content-length")) {
      int cl = atoi(headers_["content-length"].c_str());
      while (int(key.length()) > cl) {
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

int HttpRequest::ParseLastLine(char* buf, int start, int end) {
  TrimSpace(buf, start, end);
  if (unlikely(start > end)) return ST_ERROR;

  DLOG(INFO) << "Last line: " << string(buf, start, end-start+1);

  if (method_ == "post" && (!headers_.count("content-length")
        || atoi(headers_["content-length"].c_str()))) {
    string key = string(buf, start, end-start+1);
    if (headers_.count("content-length")) {
      int cl = atoi(headers_["content-length"].c_str());
      while (int(key.length()) > cl) {
        LOG(WARNING) << "Content larger than the specified content-length:"
                     << key.length() << ":" << cl;
        key.pop_back();
      }
    }
    headers_[kParaKey] = key;
  } else {
    int is = start, ie = FindChar(buf, start, end, ':');
    if (ie < end) ToLower(buf, is, ie-1);
    string key = string(buf, is, ie-is);
    ie++;
    TrimSpace(buf, ie, end);
    if (unlikely(ie > end)) {
      if (headers_.count("content-length")) {
        int cl = atoi(headers_["content-length"].c_str());
        while (int(key.length()) > cl) {
          LOG(WARNING) << "Content larger than the specified content-length";
          key.pop_back();
        }
      }
      headers_[kParaKey] = key;
    } else {
      TrimSpecialReverse(buf, end, ie);
      headers_[key] = string(buf, ie, end-ie+1);
      // LOG(WARNING) << "Parsed: " << key << " : " << headers_[key];
    }
  }
  return ST_SUCCESS;
}


unordered_map<string, string> HttpRequest::ParseParameters() {
  unordered_map<string, string> kv;
  if (headers_.count(kParaKey)) {
    string& para = headers_[kParaKey];
    if (headers_.count("content-type")
        && headers_["content-type"] == "application/xml") {
      DLOG(INFO) << "content-type: application/xml";
      DLOG(INFO) << "para: " + para;
      size_t cur = 0, prev = 0;
      bool outerFlag = 1;
      while ((cur = para.find('<', cur)) != std::string::npos) {
        if (para[cur + 1] == '?') {
          cur = para.find(">", cur + 2);
          if (cur == std::string::npos) {
            LOG(WARNING) << "XML format error";
          }
          continue;
        }
        if (para.substr(cur, 4) == "<!--") {
          cur = para.find("-->", cur + 4);
          if (cur == std::string::npos) {
            LOG(WARNING) << "XML format error";
          }
          continue;
        }
        prev = para.find('>', cur);
        if (prev == std::string::npos) {
          LOG(WARNING) << "XML format error";
          break;
        }
        string key = para.substr(cur + 1, prev - cur - 1);
        cur = para.find("</" + key + ">", prev);
        if (cur == std::string::npos) {
          LOG(WARNING) << "XML format error";
          break;
        }
        if (outerFlag) {
          para = para.substr(prev + 1, cur - prev - 1);
          cur = 0;
          outerFlag = 0;
          continue;
        }
        kv[key] = para.substr(prev + 1, cur - prev - 1);

        cur = para.find('>', cur + 1);
      }

    } else if (headers_.count("content-type")
        && headers_["content-type"] == "application/json") {
      DLOG(INFO) << "content-type: application/json";
      DLOG(INFO) << "para: " + para;
      size_t prev = para.find('{', 0) + 1;
      size_t cur = prev, colon = prev;
      while ((colon = para.find(':', cur)) != std::string::npos) {
        cur = para.find(',', colon);
        if (cur == std::string::npos) cur = para.find('}', colon);
        if (cur == std::string::npos) {
          LOG(WARNING) << "json format error";
          break;
        }
        size_t valueHead = colon + 1;
        size_t valueTail = cur - 1;
        colon--;
        TrimSpecial(para, prev, colon);
        TrimSpecial(para, valueHead, valueTail);
        kv[para.substr(prev, colon - prev + 1)]
            = para.substr(valueHead, valueTail - valueHead + 1);
        prev = cur + 1;
      }
    } else {
      DLOG(INFO) << "content-type: application/x-www-form-urlencoded";
      DLOG(INFO) << "para: " + para;
      size_t cur = 0, prev = 0;
      while ((cur = para.find('&', cur)) != std::string::npos) {
        // LOG(WARNING) << para.substr(prev, cur-prev);
        size_t ep = para.find('=', prev);
        if (ep == std::string::npos || ep >= cur) {
          LOG(WARNING) << "url format error";
          break;
        }
        CHECK_LT(ep, cur);
        kv[para.substr(prev, ep-prev)] = para.substr(ep+1, cur-ep-1);
        cur++;
        prev = cur;
      }
      size_t ep = para.find('=', prev);
      if (ep != std::string::npos) {
        CHECK_NE(ep, cur);
        kv[para.substr(prev, ep-prev)] = para.substr(ep+1);
      }
    }
  }
  return kv;
}

int HttpRequest::ReadAndParse(ClientSocket* socket) {
  char buf[kMaxHeaderSize];
  int nread = socket->Recv(buf, kMaxHeaderSize);
  if (unlikely(nread <= 0)) {  // remote has close the socket
     return ST_CLOSED;
  }

  DLOG(INFO) << "Received: " << buf;

  int ls = 0, le = 0;  // start and end position of each line
  int pos = 0;
  int linenum = 0;
  TrimSpecial(buf, pos, nread-1);
  while (likely(pos < nread)) {
    if (unlikely(pos >= nread)) break;
    ls = le = pos;
    le = FindChar(buf, ls, nread-1, '\n');  // find the line breaker \r\n or \n
    // buf[le-1] == '\n' or le == nread, anyway, buf[ls,le-1] forms a line
    assert(le > ls);
    le--;  // backwards 1 position (le == nread OR buf[le] = '\n')
    pos = le+1;

    TrimSpecial(buf, pos, nread-1);
    TrimSpecialReverse(buf, le, ls);
    if (unlikely(linenum == 0)) {
      if (ParseFirstLine(buf, ls, le) == ST_ERROR) return ST_ERROR;
    } else if (pos >= nread) {
      if (ParseLastLine(buf, ls, le) == ST_ERROR) return ST_ERROR;
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

int HttpRequest::Respond(ClientSocket* socket, std::vector<string>& response) {
  if (!(method_ == "post" || method_ == "get")) {
    if (unlikely(int(kBadRequest.length()) !=
        socket->Send(kBadRequest.data(), kBadRequest.length()))) {
      return ST_ERROR;
    }
    LOG(WARNING) << "unsupported method: " << method_;
    return ST_SUCCESS;
  }

  string prefix, suffix;
  // handle accept type
  if (headers_.count("accept")) {
    string& accept = headers_["accept"];
    if (accept == "application/xml") {
      // response = "<?xml version=\"1.0\" ?>" + CRLF + "<result>"
      //     + response + "</result>";
      prefix = "<?xml version=\"1.0\" ?>" + CRLF + "<result>";
      suffix = "</result>";
    } else if (accept == "application/json") {
      // response = "{\"result\": \"" + response + "\"}";
      prefix = "{\"result\": ";
      suffix = " }";
      // return a json array
      if (response.size() > 1) {
        prefix += "[";
        suffix = "]" + suffix;
      }
      // wrap elements
      for (size_t i = 0; i < response.size(); ++i) {
        response[i] = "\"" + response[i] + "\"";
        if (i+1 < response.size()) response[i] += ",";
      }
    }
  }

  // add CRLF ending for each line
  if (prefix.length()) prefix += CRLF;
  if (suffix.length()) suffix += CRLF;
  size_t res_len = prefix.length() + suffix.length();
  for (auto& s : response) {
    if (s.length()) s += CRLF;
    res_len += s.length();
  }

  // bound large message
  if (res_len > kMaxOutputSize) {
    LOG(WARNING) << "response length is too long: " << res_len
                 << ", cut it to " << kMaxOutputSize;
    res_len = kMaxOutputSize;
  }

  // dynamic buffer allocation
  char* header = new char[kMaxHeaderSize + res_len];
  int pos = 0;

  memcpy(header+pos, kHttpVersion.data(), kHttpVersion.length());
  pos += kHttpVersion.length();
  memcpy(header+pos, status_.data(), status_.length());
  pos += status_.length();
  memcpy(header+pos, kOtherHeaders.data(), kOtherHeaders.length());
  pos += kOtherHeaders.length();
  memcpy(header+pos, kContentLen.data(), kContentLen.length());
  pos += kContentLen.length();

  // end of header
  pos += sprintf(header+pos, "%zu\r\n\r\n", res_len);

  // copy message body
  memcpy(header+pos, prefix.data(), prefix.length());
  pos += prefix.length();
  size_t remain = res_len - prefix.length() - suffix.length();
  for (const auto& s : response) {
    if (!remain) break;
    size_t len = s.length();
    if (len > remain) len = remain;
    memcpy(header+pos, s.data(), len);
    pos += len;
    remain -= len;
  }
  memcpy(header+pos, suffix.data(), suffix.length());
  pos += suffix.length();

  int sent = socket->Send(header, pos);
  delete[] header;
  return sent == pos ? ST_SUCCESS : ST_ERROR;
}

}  // namespace ustore

