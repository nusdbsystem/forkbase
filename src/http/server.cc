// Copyright (c) 2017 The Ustore Authors.

#include <types/type.h>
#include <iostream>
#include <string>
#include "utils/logging.h"
#include "http/net.h"
#include "http/event.h"
#include "http/http_request.h"
#include "http/server.h"
#include "http/http_client_service.h"

using std::string;
using std::cout;
using std::endl;

namespace ustore {

/*
 * event handler to process request from clients
 * el: EventLoop pointer
 * fd: file descriptor of the ClientSocket
 * data: ClientSocket pointer
 * mask: READABLE / WRITABLE
 */
void ProcessTcpClientHandle(EventLoop *el, int fd, void *data, int mask) {
  ClientSocket* cs = static_cast<ClientSocket*>(data);
  // LOG(LOG_WARNING, "Process client = %d", cs->GetFD());

  HttpRequest request;
  int status = request.ReadAndParse(cs);
  if (status == ST_CLOSED) {
    delete cs;
    el->DeleteFileEvent(fd, READABLE);
  } else if (status == ST_ERROR) {
    LOG(WARNING)<< "Parse request failed";
  } else {
    unordered_map<string, string> paras = request.GetParameters();
    for (const auto& it : paras) {
      LOG(INFO) << it.first << ":" << it.second;
    }

    string response;
    switch (request.GetCommand()) {
      case GET:
      LOG(INFO) << "Get Command";
      CHECK(paras.count("key"));
      if (paras.count("version")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Get(Slice(paras["key"]), Hash::FromBase32(paras["version"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.String().slice().ToString();
        } else {
          response = "Get Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else if (paras.count("branch")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Get(Slice(paras["key"]), Slice(paras["branch"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.String().slice().ToString();
        } else {
          response = "Get Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else {
        response = "Get parameter error";
        LOG(WARNING) << response;
      }
      break;
      case PUT:
      LOG(INFO) << "Put Command";
      CHECK(paras.count("key") && paras.count("value"));
      if (paras.count("version")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
            ->Put(Slice(paras["key"]), VString(Slice(paras["value"])),
                  Hash::FromBase32(paras["version"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.version().ToBase32();
        } else {
          response = "Put Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else if (paras.count("branch")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Put(Slice(paras["key"]),
              VString(Slice(paras["value"])), Slice(paras["branch"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.version().ToBase32();
        } else {
          response = "Put Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else {
        response = "Put parameter error";
        LOG(WARNING) << response;
      }
      break;
      case MERGE:
      LOG(INFO) << "Merge Command";
      CHECK(paras.count("key") && paras.count("value"));
      if (paras.count("tgt_branch") && paras.count("ref_branch")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Merge(Slice(paras["key"]), VString(Slice(paras["value"])),
            Slice(paras["tgt_branch"]), Slice(paras["ref_branch"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.version().ToBase32();
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else if (paras.count("tgt_branch") && paras.count("ref_version1")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Merge(Slice(paras["key"]), VString(Slice(paras["value"])),
            Slice(paras["tgt_branch"]), Hash::FromBase32(paras["ref_version1"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.version().ToBase32();
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else if (paras.count("ref_version1") && paras.count("ref_version2")) {
        VMeta meta = HttpClientService::Instance()->GetDB()
        ->Merge(Slice(paras["key"]), VString(Slice(paras["value"])),
            Hash::FromBase32(paras["ref_version1"]),
            Hash::FromBase32(paras["ref_version2"]));
        if (meta.code() == ErrorCode::kOK) {
          response = meta.version().ToBase32();
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(meta.code()));
        }
      } else {
        response = "Merge parameter error";
        LOG(WARNING) << response;
      }
      break;
      case BRANCH:
      LOG(INFO) << "Branch Command";
      CHECK(paras.count("key") && paras.count("new_branch"));
      if (paras.count("old_branch")) {
        ErrorCode code = HttpClientService::Instance()->GetDB()
        ->Branch(Slice(paras["key"]), Slice(paras["old_branch"]),
                 Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK";
        } else {
          response = "Branch Error: " + std::to_string(static_cast<int>(code));
        }
      } else if (paras.count("version")) {
        ErrorCode code = HttpClientService::Instance()->GetDB()
        ->Branch(Slice(paras["key"]), Hash::FromBase32(paras["version"]),
                 Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK";
        } else {
          response = "Branch Error: " + std::to_string(static_cast<int>(code));
        }
      } else {
        response = "Branch parameter error";
        LOG(WARNING) << response;
      }
      break;
      case RENAME:
      LOG(INFO) << "Rename Command";
      if (paras.count("key") && paras.count("new_branch")
          && paras.count("old_branch")) {
        ErrorCode code = HttpClientService::Instance()->GetDB()
        ->Branch(Slice(paras["key"]), Slice(paras["old_branch"]),
                 Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK";
        } else {
          response = "Rename Error: " + std::to_string(static_cast<int>(code));
        }
      } else {
        response = "Rename parameter error";
        LOG(WARNING) << response;
      }
      break;
      case LIST:
      response = "Not Support LIST";
      LOG(WARNING) << response;
      break;
      case HEAD:
      response = "Not Support HEAD";
      LOG(WARNING) << response;
      break;
      case LATEST:
      response = "Not Support LATEST";
      LOG(WARNING) << response;
      break;
      default:
      response = "Unrecognized uri: " + request.GetCommand();
      LOG(WARNING) << response;
    }

    if (request.Respond(cs, response) != ST_SUCCESS) {
      LOG(WARNING) << "respond to client failed";
    }

    if (!request.KeepAlive()) {
      LOG(INFO) << "not keep alive, delete socket";
      delete cs;
      el->DeleteFileEvent(fd, READABLE);
    }
  }
}

/*
 * event handler to accept connection from clients
 * el: EventLoop pointer
 * fd: file descriptor of the ClientSocket
 * data: HttpServer pointer
 * mask: READABLE / WRITABLE
 */
void AcceptTcpClientHandle(EventLoop *el, int fd, void *data, int mask) {
  HttpServer* ser = static_cast<HttpServer*>(data);
  ClientSocket* cs = ser->GetServerSocket().Accept();
  if (!cs) {
    LOG(FATAL)<< "cannot accept client";
    return;
  }
  LOG(INFO)<< "Accept client = " << cs->GetFD();

  if (ser->DispatchClientSocket(cs) == ST_ERROR) {
    LOG(FATAL) << "dispatch client socket failed";
    return;
  }
}

// wrapper function to facilitate to create a thread
void StartEventLoopThread(EventLoop* el) {
  el->Start();
}

HttpServer::HttpServer(int port, const string& bind_addr, int backlog)
    : ss_(port, bind_addr, backlog) {
}

HttpServer::~HttpServer() {
  if (el_) {
    delete el_[0];
    for (int i = 1; i < threads_num_; i++) {
      delete el_[i];
      delete ethreads_[i];
    }
  }
}

int HttpServer::Start(int threads_num) {
  int st = ss_.Listen();
  if (st == ST_ERROR)
    return st;

  threads_num_ = threads_num;
  el_ = new EventLoop*[threads_num_];
  if (threads_num_ > 1)
    ethreads_ = new std::thread*[threads_num_ - 1];
  for (int i = 0; i < threads_num_; i++) {
    el_[i] = new EventLoop(el_size_);
  }

  if (el_[0]->CreateFileEvent(ss_.GetFD(), READABLE, AcceptTcpClientHandle,
                              this) == ST_ERROR) {
    LOG(FATAL)<< "cannot create file event";
    return ST_ERROR;
  }

  for (int i = 1; i < threads_num_; i++) {
    ethreads_[i - 1] = new std::thread(StartEventLoopThread, el_[i]);
  }
  el_[0]->Start();
  return st;
}

// TODO(zhanghao): load balance
int HttpServer::DispatchClientSocket(ClientSocket* cs) {
  int hash = cs->GetFD() % threads_num_;
  EventLoop* el = el_[hash];
  if (el->CreateFileEvent(cs->GetFD(), READABLE, ProcessTcpClientHandle,
                          cs) == ST_ERROR) {
    LOG(FATAL) << "cannot create file event";
    return ST_ERROR;
  }
  return ST_SUCCESS;
}

}  // namespace ustore
