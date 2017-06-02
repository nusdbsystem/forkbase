// Copyright (c) 2017 The Ustore Authors.

#include <types/type.h>
#include <iostream>
#include <string>
#include "utils/logging.h"
#include "http/net.h"
#include "http/event.h"
#include "http/http_request.h"
#include "http/server.h"

namespace ustore {

using std::string;
using std::cout;
using std::endl;
using std::unordered_map;

/*
 * event handler to process request from clients
 * el: EventLoop pointer
 * fd: file descriptor of the ClientSocket
 * data: ClientSocket pointer
 * mask: kReadable / kWritable
 */
void ProcessTcpClientHandle(EventLoop *el, int fd, void *data, int mask) {
  HttpServer* hserver = static_cast<HttpServer*>(data);
  ClientSocket* cs = hserver->GetClientSocket(fd);
  // LOG(LOG_WARNING, "Process client = %d", cs->GetFD());

  HttpRequest request;
  int status = request.ReadAndParse(cs);
  if (status == ST_CLOSED) {
    hserver->Close(cs);
    el->DeleteFileEvent(fd, kReadable);
  } else if (status == ST_ERROR) {
    LOG(WARNING)<< "Parse request failed";
  } else {
    unordered_map<string, string> paras = request.GetParameters();
    for (const auto& it : paras) {
      DLOG(INFO) << it.first << ":" << it.second;
    }

    string response;
    switch (request.GetCommand()) {
      case CommandType::kGet:
      DLOG(INFO) << "Get Command";
      if (!paras.count("key")) {
        response = "No key provided" + CRLF;
        break;
      }
      if (paras.count("version")) {
        auto rlt = hserver->GetODB()
        .Get(Slice(paras["key"]), Hash::FromBase32(paras["version"]));
        if (rlt.stat == ErrorCode::kOK) {
          // response = rlt.value.String().slice().ToString() + CRLF;
          std::stringstream ss;
          ss << rlt.value << CRLF;
          response = ss.str();
        } else {
          response = "Get Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("branch")) {
        auto rlt = hserver->GetODB()
        .Get(Slice(paras["key"]), Slice(paras["branch"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.String().slice().ToString() + CRLF;
        } else {
          response = "Get Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Get parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kPut:
      DLOG(INFO) << "Put Command";
      if (!paras.count("key") || !paras.count("value")) {
        response = "No key or value provided" + CRLF;
        break;
      }
      if (paras.count("version")) {
        auto rlt = hserver->GetODB()
            .Put(Slice(paras["key"]), VString(Slice(paras["value"])),
                  Hash::FromBase32(paras["version"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Put Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("branch")) {
        auto rlt = hserver->GetODB()
        .Put(Slice(paras["key"]),
              VString(Slice(paras["value"])), Slice(paras["branch"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Put Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Put parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kMerge:
      DLOG(INFO) << "Merge Command";
      if (!paras.count("key") || !paras.count("value")) {
        response = "No key or value provided" + CRLF;
        break;
      }
      if (paras.count("tgt_branch") && paras.count("ref_branch")) {
        auto rlt = hserver->GetODB()
        .Merge(Slice(paras["key"]), VString(Slice(paras["value"])),
            Slice(paras["tgt_branch"]), Slice(paras["ref_branch"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("tgt_branch") && paras.count("ref_version1")) {
        auto rlt = hserver->GetODB().Merge(Slice(paras["key"]),
            VString(Slice(paras["value"])),
            Slice(paras["tgt_branch"]),
            Hash::FromBase32(paras["ref_version1"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("ref_version1") && paras.count("ref_version2")) {
        auto rlt = hserver->GetODB()
        .Merge(Slice(paras["key"]), VString(Slice(paras["value"])),
            Hash::FromBase32(paras["ref_version1"]),
            Hash::FromBase32(paras["ref_version2"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Merge Error: " +
              std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Merge parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kBranch:
      DLOG(INFO) << "Branch Command";
      if (!paras.count("key") || !paras.count("new_branch")) {
        response = "No key or new_branch provided" + CRLF;
        break;
      }
      if (paras.count("old_branch")) {
        auto code = hserver->GetODB()
        .Branch(Slice(paras["key"]), Slice(paras["old_branch"]),
                Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK" + CRLF;
        } else {
          response = "Branch Error: " + std::to_string(static_cast<int>(code))
                   + CRLF;
        }
      } else if (paras.count("version")) {
        auto code = hserver->GetODB()
        .Branch(Slice(paras["key"]), Hash::FromBase32(paras["version"]),
                Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK" + CRLF;
        } else {
          response = "Branch Error: " + std::to_string(static_cast<int>(code))
                   + CRLF;
        }
      } else {
        response = "Branch parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kRename:
      DLOG(INFO) << "Rename Command";
      if (paras.count("key") && paras.count("new_branch")
          && paras.count("old_branch")) {
        auto code = hserver->GetODB()
        .Rename(Slice(paras["key"]), Slice(paras["old_branch"]),
                Slice(paras["new_branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK" + CRLF;
        } else {
          response = "Rename Error: " + std::to_string(static_cast<int>(code))
                   + CRLF;
        }
      } else {
        response = "Rename parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kDelete:
      DLOG(INFO) << "Delete Command";
      if (paras.count("key") && paras.count("branch")) {
        auto code = hserver->GetODB()
        .Delete(Slice(paras["key"]), Slice(paras["branch"]));
        if (code == ErrorCode::kOK) {
          response = "OK" + CRLF;
        } else {
          response = "Delete Error: " + std::to_string(static_cast<int>(code))
                   + CRLF;
        }
      } else {
        response = "Delete parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kList:
      DLOG(INFO) << "List Command";
      // list all keys
      if (request.GetMethod() == "get") {
        auto rlt = hserver->GetODB().ListKeys();
        if (rlt.stat == ErrorCode::kOK) {
          for (std::string& v : rlt.value) {
            response += v + CRLF;
          }
        } else {
          response = "List Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("key")) {  // list all branches of a key
        auto rlt = hserver->GetODB().ListBranches(Slice(paras["key"]));
        if (rlt.stat == ErrorCode::kOK) {
          for (std::string& v : rlt.value) {
            response += v + CRLF;
          }
        } else {
          response = "List Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "List parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kHead:
      DLOG(INFO) << "Head Command";
      if (paras.count("key") && paras.count("branch")) {
        auto rlt = hserver->GetODB().GetBranchHead(Slice(paras["key"]),
                                                   Slice(paras["branch"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value.ToBase32() + CRLF;
        } else {
          response = "Head Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Head parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kLatest:
      DLOG(INFO) << "Latest Command";
      if (paras.count("key")) {
        auto rlt = hserver->GetODB().GetLatestVersions(Slice(paras["key"]));
        if (rlt.stat == ErrorCode::kOK) {
          for (Hash& h : rlt.value) {
            response += h.ToBase32() + CRLF;
          }
        } else {
          response = "Latest Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Latest parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kExists:
      DLOG(INFO) << "Exists Command";
      if (paras.count("key") && paras.count("branch")) {
        auto rlt = hserver->GetODB().Exists(Slice(paras["key"]),
                                            Slice(paras["branch"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value == true ? "true" : "false";
          response += CRLF;
        } else {
          response = "Exists Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else if (paras.count("key")) {
        auto rlt = hserver->GetODB().Exists(Slice(paras["key"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value == true ? "true" : "false";
          response += CRLF;
        } else {
          response = "Exists Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "Exists parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kIsBranchHead:
      DLOG(INFO) << "IsBranchHead Command";
      if (paras.count("key") && paras.count("branch")
          && paras.count("version")) {
        auto rlt = hserver->GetODB().
        IsBranchHead(Slice(paras["key"]), Slice(paras["branch"]),
                     Hash::FromBase32(paras["version"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value == true ? "true" : "false";
          response += CRLF;
        } else {
          response = "IsBranchHead Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "IsBranchHead parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      case CommandType::kIsLatestVersion:
      DLOG(INFO) << "IsLatestVersion Command";
      if (paras.count("key") && paras.count("version")) {
        auto rlt = hserver->GetODB().
        IsLatestVersion(Slice(paras["key"]),
                        Hash::FromBase32(paras["version"]));
        if (rlt.stat == ErrorCode::kOK) {
          response = rlt.value == true ? "true" : "false";
          response += CRLF;
        } else {
          response = "IsLatestVersion Error: " +
          std::to_string(static_cast<int>(rlt.stat)) + CRLF;
        }
      } else {
        response = "IsLatestVersion parameter error" + CRLF;
        LOG(WARNING) << response;
      }
      break;
      default:
      response = "Unrecognized uri: "
          + std::to_string(static_cast<int>(request.GetCommand())) + CRLF;
      LOG(WARNING) << response;
    }

    if (request.Respond(cs, response) != ST_SUCCESS) {
      LOG(WARNING) << "respond to client failed";
    }

    if (!request.KeepAlive()) {
      LOG(INFO) << "not keep alive, delete socket";
      hserver->Close(cs);
      el->DeleteFileEvent(fd, kReadable);
    }
  }
}

/*
 * event handler to accept connection from clients
 * el: EventLoop pointer
 * fd: file descriptor of the ClientSocket
 * data: HttpServer pointer
 * mask: kReadable / kWritable
 */
void AcceptTcpClientHandle(EventLoop *el, int fd, void *data, int mask) {
  HttpServer* ser = static_cast<HttpServer*>(data);
  ClientSocket* cs = ser->Accept();
  if (!cs) {
    LOG(FATAL)<< "cannot accept client";
    return;
  }
  LOG(INFO) << "Accept client = " << cs->GetFD();

  if (ser->DispatchClientSocket(cs) == ST_ERROR) {
    LOG(FATAL) << "dispatch client socket failed";
    return;
  }
}

// wrapper function to facilitate to create a thread
void StartEventLoopThread(EventLoop* el) {
  el->Start();
}

HttpServer::HttpServer(DB* db, int port, const string& bind_addr, int backlog)
    : odb_(db), ss_(port, bind_addr, backlog) {
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

  if (el_[0]->CreateFileEvent(ss_.GetFD(), kReadable, AcceptTcpClientHandle,
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
  if (el->CreateFileEvent(cs->GetFD(), kReadable, ProcessTcpClientHandle,
                          this) == ST_ERROR) {
    LOG(FATAL) << "cannot create file event";
    return ST_ERROR;
  }
  return ST_SUCCESS;
}

}  // namespace ustore
