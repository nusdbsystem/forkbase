// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <stdlib.h>
#include <unistd.h>

#include "utils/http.h"

void HttpHandler::operator()(HttpServer::request const &request,
  HttpServer::response &response) {
  HttpServer::string_type ip = source(request);
  // unsigned int port = request.source_port;
  std::ostringstream data;
  string s;

  ProtoBufferNode* ptr = buffer->get_front();
  printOutputString(s, ptr);

  data << s;

  response = HttpServer::response::stock_reply(
    HttpServer::response::ok, data.str());

  static const HttpServer::response_header
    MIME_JSON = {"Content-Type", "application/json"};
  static const HttpServer::response_header
    NO_CACHE = {"Cache-Control", "no-store"};
  static const HttpServer::response_header
    ALLOW_ORIGIN = {"Access-Control-Allow-Origin", "*"};
  static const HttpServer::response_header
    ALLOW_HEADER = {"Access-Control-Allow-Headers",
      "Content-Type, Accept, Authorization, X-Requested-With"};

  response.headers.push_back(MIME_JSON);
  response.headers.push_back(NO_CACHE);
  response.headers.push_back(ALLOW_ORIGIN);
  response.headers.push_back(ALLOW_HEADER);
}

void HttpHandler::log(...) {
  // do nothing
}

void startHttpService(int port, ProtoBuffer* buf) {
  try {
    // Creates the request handler
    HttpHandler handler;
    handler.buffer = buf;
    // Creates the server
    HttpServer::options options(handler);
    HttpServer server_(options.address("0.0.0.0")
      .port(std::to_string(port).c_str()));
    // Runs the server
    server_.run();
  }
  catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }
}

void printProcInfo(string &s, ProcInfo *p) {
  char buf[500];
  static const char header[] = "{ \"pname\" : \"%s\" ,"
    " \"cpu\" : \"%.1lf\" , \"vmem\" : \"%lu\" ,"
    " \"rss\" : \"%lu\" , \"io_read\" : \"%d\" ,"
    " \"io_write\" : \"%d\" , \"net_send\" : \"%d\" ,"
    " \"net_recv\" : \"%d\" }";
  sprintf(buf, header,
      p->name, p->cpu, p->mem_v, p->mem_r, p->io_read,
      p->io_write, p->net_send, p->net_recv);

  s += string(buf);
}

void printSnapshot(string &s, ProtoBufferNode *p) {
  char buf[100];
  static const char header[] = "\"time\" : \"%lu\" ,"
    " \"hname\" : \"%s\" , \"procs\" : [ ";
  sprintf(buf, header, p->t_monitor, p->node);

  s += "{ ";
  s += string(buf);
  ProcInfo *pp = p->head;
  for (int i = 0; i < p->size; ++i) {
    if (i > 0) s += " , ";
    printProcInfo(s, &pp[i]);
  }
  s += " ] }";
}

void printOutputString(string &s, ProtoBufferNode *p) {
  int cnt = 0;

  s += "{ \"snapshots\" : [ ";
  while (p != nullptr) {
    if (cnt > 0) s += " , ";
    ++cnt;
    printSnapshot(s, p);
    p = p->next;
  }
  s += " ] }";
}
