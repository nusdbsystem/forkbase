// Copyright (c) 2017 The Ustore Authors.

#include "net/zmq_net.h"

#include <czmq.h>
#include <gflags/gflags.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <cstring>
#include <string>
#include <set>
#include <thread>
#include "utils/logging.h"

namespace ustore {

/**
 * Implementation of Network communication via ZeroMQ
 */

constexpr int kPoolTimeout = 10;
constexpr int kSocketBindTimeout = 1;
constexpr int kSocketTrials = 5;

using std::string;
using std::vector;

// server thread to process messages
void ServerThread(void *args);

Net::~Net() {
  for (auto val : netmap_)
    delete val.second;
  DLOG(INFO) << "Destroy Net ";
}

void Net::CreateNetContexts(const vector<node_id_t>& nodes) {
  std::set<node_id_t> tmp;
  for (size_t i = 0; i < nodes.size(); i++)
    tmp.insert(nodes[i]);
  for (auto val = netmap_.begin(); val != netmap_.end(); val++)
    if (tmp.find(val->first) == tmp.end()) {
      delete val->second;
    }
    else
      tmp.erase(val->first);

  for (auto val = tmp.begin(); val != tmp.end(); val++) {
    if (*val != cur_node_)
      CreateNetContext(*val);
  }
}

void Net::DeleteNetContext(NetContext* ctx) {
  delete ctx;
}


void ZmqNet::Stop() {
  is_running_ = false;
}

ZmqNetContext::ZmqNetContext(const node_id_t& src, const node_id_t& dest)
    : NetContext(src, dest) {
  // start connection to the remote host
  send_sock_ = zsock_new(ZMQ_DEALER);
  string host = "tcp://" + dest_id_;
  CHECK_EQ(zsock_connect((zsock_t *)send_sock_, "%s", host.c_str()), 0);
}

ZmqNetContext::~ZmqNetContext() {
  // Send(kCloseMsg, std::strlen(kCloseMsg));
  zsock_destroy((zsock_t **)&send_sock_);
}

ssize_t ZmqNetContext::Send(const void *ptr, size_t len, CallBack* func) {
  zframe_t* frame = zframe_new(ptr, len);
  // free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);

  send_lock_.lock();
  int st = zmsg_send(&msg, (zsock_t *)send_sock_) == 0 ? len : -1;
  send_lock_.unlock();
  return st;
}

void ZmqNet::Dispatch(const node_id_t& source, const void *msg, int size) {
  // recv_lock_.lock();
  /*
  if (memcmp(kCloseMsg, msg, size) == 0) {
    DLOG(WARNING) << "Close ZmqNetContext " << source;
    DeleteNetContext(source);
    return;
  }
  */
  (*this->cb_)(msg, size, source);
  // recv_lock_.unlock();
}

ssize_t NetContext::SyncSend(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}

ssize_t NetContext::SyncRecv(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}


// ClientZMQ constructor
ClientZmqNet::ClientZmqNet(int nthreads)
    : ZmqNet("", nthreads) {
  int status;
  int ntries = kSocketTrials;
  srand(time(NULL));
  int port = rand() % 0xffffff;
  inproc_ep_ = "inproc://"  + std::to_string(port);

  // make sure that the ipc socket binds successfully
  backend_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)backend_sock_, "%s",
          inproc_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    port = rand() & 0xffffff;
    inproc_ep_ = "inproc://"  + std::to_string(port);
  }
  CHECK_EQ(status, 0);

  is_running_ = true;
}

NetContext* ZmqNet::CreateNetContext(const node_id_t& id) {
  ZmqNetContext* ctx = nullptr;
  if (!netmap_.count(id)) {
    ctx = new ZmqNetContext(cur_node_, id);
    netmap_[id] = ctx;
  }
  CHECK(netmap_[id]) << "Creating netcontext failed";
  return netmap_[id];
}

void ClientZmqNet::Start() {
  // start backend thread
  for (int i = 0; i < nthreads_; i++)
    backend_threads_.push_back(
              std::thread(&ClientZmqNet::ClientThread, this));

  bool first = true;
  zpoller_t *zpoller; 
  for (auto k : netmap_) {
    ZmqNetContext *nctx = reinterpret_cast<ZmqNetContext*>(k.second);
//                              (netmap_[(const node_id_t&)k]);
    if (first)
      zpoller = zpoller_new(nctx->GetSocket(), NULL);
    else
      zpoller_add(zpoller, nctx->GetSocket());
  }

  while (is_running_) {
    void *sock = zpoller_wait(zpoller, kPoolTimeout);
    if (!sock && !zpoller_expired(zpoller))
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg)
        break;
      // send to backend
      zmsg_send(&msg, backend_sock_);
    }
  }
  // Stop when ^C
  zsock_destroy((zsock_t **)&backend_sock_);
  zpoller_destroy(&zpoller);
  for (int i=0; i < backend_threads_.size(); i++)
    backend_threads_[i].join();
}

void ClientZmqNet::ClientThread() {
  // create and connect to the frontend's dealer socket
  void *frontend = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)frontend,
           "%s", inproc_ep_.c_str()), 0);
  zpoller_t *zpoller = zpoller_new(frontend, NULL);
  while (IsRunning()) {
    void *sock = zpoller_wait(zpoller, kPoolTimeout);
    if (!sock && !zpoller_expired(zpoller))
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg)
        break;

      // message processing
      // first frame is connection ID
      // final frame is the message itself
      char *connection_id = zmsg_popstr(msg);

      zframe_t *content = zmsg_pop(msg);
      Dispatch(string(connection_id),
          zframe_data(content), zframe_size(content));
      zframe_destroy(&content);
      free(connection_id);
    }
  }
  zsock_destroy((zsock_t **)&frontend);

}

ServerZmqNet::ServerZmqNet(const node_id_t& id, int nthreads)
    : ZmqNet(id, nthreads) {
  int status;
  int ntries = kSocketTrials;

  // start router socket
  recv_sock_ = zsock_new(ZMQ_ROUTER);

  int split = id.find(':');
  string hostname = id.substr(0, split);
  string port = id.substr(split+1);
  hostent* record = gethostbyname(hostname.c_str());
  if (record == nullptr) {
    LOG(ERROR) << hostname <<  " is unavailable\n";
    exit(1);
  }
  in_addr* address = reinterpret_cast<in_addr*>(record->h_addr);
  string ip_address = inet_ntoa(*address);
  string host = "tcp://" + ip_address + ":" + port;
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)recv_sock_, "%s",
          host.c_str())) < 0 && ntries--) 
    sleep(kSocketBindTimeout);
  CHECK_GT(status, 0);

  srand(time(NULL));
  int iport = rand() & 0xffffff;
  inproc_ep_ = "inproc://"  + std::to_string(iport);

  // backend socket
  backend_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)backend_sock_, "%s",
          inproc_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    iport = rand() & 0xffffff;
    inproc_ep_ = "inproc://"  + std::to_string(iport);
  }
  CHECK_EQ(status, 0);

  srand(time(NULL));
  iport = rand() & 0xffffff;
  result_ep_ = "inproc://" + std::to_string(iport);

  // result socket
  result_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)result_sock_, "%s",
          result_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    iport = rand() & 0xffffff;
    result_ep_ = "inproc://"  + std::to_string(iport);
  }
  CHECK_EQ(status, 0);

  is_running_ = true;
}

void ServerZmqNet::Start() {
  // start backend thread
  for (int i=0; i < nthreads_; i++) {
    CHECK_EQ(netmap_.count(std::to_string(i)), 0);
    ServerZmqNetContext *nctx = 
              new ServerZmqNetContext("","", inproc_ep_, result_ep_,
                                    std::to_string(i));
    netmap_[std::to_string(i)] = nctx;
    backend_threads_.push_back(
                    std::thread(&ServerZmqNetContext::Start, nctx, this));
  }
  
  zpoller_t *zpoller = zpoller_new(recv_sock_, result_sock_, NULL);
  CHECK_NOTNULL(zpoller);

  while (is_running_) {
    void *sock = zpoller_wait(zpoller, kPoolTimeout);
    if (!sock && !zpoller_expired(zpoller))
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg)
        break;

      // send to backend
      sock == recv_sock_ ? zmsg_send(&msg, backend_sock_)
                         : zmsg_send(&msg, recv_sock_);
    } 
  }
  // Stop when ^C
  zsock_destroy((zsock_t **)&recv_sock_);
  zsock_destroy((zsock_t **)&backend_sock_);
  zsock_destroy((zsock_t **)&result_sock_);
  zpoller_destroy(&zpoller);

  for (int i=0; i < backend_threads_.size(); i++)
    backend_threads_[i].join();
}


ServerZmqNetContext::ServerZmqNetContext(const node_id_t& src,
                    const node_id_t& dest, const string& ipc_ep,
                    const string& result_ep, const string& id) 
                             : NetContext(src, dest), id_(id) {
  recv_sock_ = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)recv_sock_, "%s", ipc_ep.c_str()), 0);
  send_sock_ = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)send_sock_, "%s", result_ep.c_str()), 0);

}

ssize_t ServerZmqNetContext::Send(const void *ptr, size_t len,
                                                    CallBack* func) {
  // append id frame
  CHECK_NOTNULL(client_id_);

  zframe_t* frame = zframe_new(ptr, len);
  // free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);
  
  zmsg_prepend(msg, reinterpret_cast<zframe_t**>(&client_id_));
  send_lock_.lock();
  int st = zmsg_send(&msg, (zsock_t *)send_sock_) == 0 ? len : -1;
  send_lock_.unlock();
  return st;

}

void ServerZmqNetContext::Start(ServerZmqNet *net) {

  // listen from  recv_sock_
  zpoller_t *zpoller = zpoller_new(recv_sock_, NULL);
  while (net->IsRunning()) {
    void *sock = zpoller_wait(zpoller, kPoolTimeout);
    if (!sock && !zpoller_expired(zpoller))
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg)
        break;

      // message processing
      // first frame is the identity (from router)
      // next contain connection ID
      // final frame is the message itself
      client_id_ = zmsg_pop(msg);

      char *connection_id = zmsg_popstr(msg);
      
      zframe_t *content = zmsg_pop(msg);
      // if there's a response, it will be forwarded by this thread
      net->Dispatch(id_, zframe_data(content), zframe_size(content));
      zframe_destroy(&content);
      free(connection_id);
    }
  }
  zsock_destroy((zsock_t **)&send_sock_);
  zsock_destroy((zsock_t **)&recv_sock_);
}

}  // namespace ustore
