// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_UTILS_HTTP_H_
#define USTORE_PERFMON_UTILS_HTTP_H_

#include <boost/network/protocol/http/server.hpp>
#include <string>
#include "utils/protobuf.h"

using std::string;

namespace http = boost::network::http;

struct HttpHandler;

typedef http::server<HttpHandler> HttpServer;

struct HttpHandler{
  ProtoBuffer *buffer;
  void operator()(HttpServer::request const &request,
    HttpServer::response &response);
  void log(...);
};

void startHttpService(int port, ProtoBuffer *buf);
void printOutputString(string &s, ProtoBufferNode *p);
void printSnapshot(string &s, ProtoBufferNode *p);
void printProcInfo(string &s, ProcInfo *p);

#endif  // USTORE_PERFMON_UTILS_HTTP_H_
