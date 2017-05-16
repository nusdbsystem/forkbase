#ifndef INCLUDE_HTTP_H
#define INCLUDE_HTTP_H

#include <string>
#include <boost/network/protocol/http/server.hpp>
#include "utils/protobuf.h"

using std::string;

namespace http = boost::network::http;

struct HttpHandler;

typedef http::server<HttpHandler> HttpServer;

struct HttpHandler{
  ProtoBuffer *buffer;
  void operator()(HttpServer::request const &request, HttpServer::response &response);
  void log(...);
};

void startHttpService(int port, ProtoBuffer *buf);
void printOutputString(string &s, ProtoBufferNode *p);
void printSnapshot(string &s, ProtoBufferNode *p);
void printProcInfo(string &s, ProcInfo *p);

#endif
