// Copyright (c) 2017 The Ustore Authors.

#include <gflags/gflags.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include "net/net.h"
#include "utils/logging.h"
#include "gtest/gtest.h"

// by default, it choose the zmq_net
// if u want to test the rdma net, you have to define USE_RDMA
// #define USE_RDMA

#ifdef USE_RDMA
#include "net/rdma_net.h"
#else
#include "net/zmq_net.h"
#endif

using namespace ustore;
using std::string;
using std::thread;
using std::vector;

const int kSleepTime = 100000;
const string kID0 = "localhost:2235";
const string kID1 = "localhost:2236";
const string kID2 = "localhost:2237";

class TestCallBack : public CallBack {
 public:
  explicit TestCallBack(void* handler = nullptr): CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) {
    string m(static_cast<const char*>(msg), size);
    //EXPECT_TRUE(source.compare(m) == 0);
    //EXPECT_TRUE((m == kID0) || (m == kID1) || (m == kID2));
    DLOG(INFO) << "msg = " << m;
  }
};

class ServerCallBack : public CallBack {
 public:
  explicit ServerCallBack(void* handler): CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) {
    Net* net = static_cast<Net*>(handler_);
    string m(static_cast<const char*>(msg), size);
    //EXPECT_TRUE(source.compare(m) == 0);
    //EXPECT_TRUE((m == kID0) || (m == kID1) || (m == kID2));
    DLOG(INFO) << "Server received msg = " << m;
    net->GetNetContext(source)->Send(m.c_str(), m.size());
  }
};

void Start(Net* net) {
  net->Start();
}

// test putting two value from node0 to node1 and node2
TEST(NetTest, MsgTest) {
#ifdef USE_RDMA
  Net* net0 = new RdmaNet(kID0);
  Net* net1 = new RdmaNet(kID1);
  Net* net2 = new RdmaNet(kID2);
#else
  Net* net0 = new ServerZmqNet(kID0);
  Net* net1 = new ServerZmqNet(kID1);
  Net* net2 = new ServerZmqNet(kID2);
  Net *client = new ClientZmqNet();
#endif

  usleep(kSleepTime);
#ifdef USE_RDMA
  NetContext* ctx0_1 = net0->CreateNetContext(kID1);
  NetContext* ctx0_2 = net0->CreateNetContext(kID2);
  
  /*
   * create NetContext from node 1
   * actually this is not necessary since node 0 already connected to node 1
   * but it is ok to do this connection again (will be ignored)
   */
  NetContext* ctx1_0 = net1->CreateNetContext(kID0);
  NetContext* ctx1_2 = net1->CreateNetContext(kID2);

  // create the NetContext from node 2
  NetContext* ctx2_0 = net2->CreateNetContext(kID0);
  NetContext* ctx2_1 = net2->CreateNetContext(kID1);
#else
  NetContext* ctx1 = client->CreateNetContext(kID1);
  NetContext* ctx2 = client->CreateNetContext(kID2);
  NetContext* ctx3 = client->CreateNetContext(kID2);
#endif
  TestCallBack cb0;
  TestCallBack cb1;
  TestCallBack cb2;

  // register receive callback function
  net0->RegisterRecv(&cb0);
  net1->RegisterRecv(&cb1);
  net2->RegisterRecv(&cb2);

  usleep(kSleepTime);

  // start the net background thread
  thread* t0 = new thread(Start, net0);
  thread* t1 = new thread(Start, net1);
  thread* t2 = new thread(Start, net2);
#ifndef USE_RDMA
  thread* t3 = new thread(Start, client);
#endif

#ifdef USE_RDMA
  /*
   * node 0 sends msg to node 1
   */
  ctx0_1->Send(kID0.c_str(), kID0.length());

  /*
   * node 1 sends msg to node 0
   */
  ctx1_0->Send(kID1.c_str(), kID1.length());

  /*
   * node 2 sends msg to node 0
   */
  ctx2_0->Send(kID2.c_str(), kID2.length());

  /*
   * node 2 sends msg to node 1
   */
  ctx2_1->Send(kID2.c_str(), kID2.length());
#else
  ctx1->Send(kID1.c_str(), kID1.length());

  ctx2->Send(kID2.c_str(), kID2.length());

  ctx3->Send(kID2.c_str(), kID2.length());

#endif
  // free the resources
  usleep(kSleepTime);
  net0->Stop();
  t0->join();
  delete net0;

  usleep(kSleepTime);
  net1->Stop();
  t1->join();
  delete net1;
  usleep(kSleepTime);

  net2->Stop();
  t2->join();
  delete net2;
  usleep(kSleepTime);
#ifndef USE_RDMA
  client->Stop();
  t3->join();
  delete client;
#endif
}

// test client and server context
 TEST(NetTest, ClientServer) {
 #ifdef USE_RDMA
   Net* server = new RdmaNet(kID0);
   Net* client = new RdmaNet("");
 #else
   Net* server = new ServerZmqNet(kID0);
   Net* client = new ClientZmqNet();
 #endif

   usleep(kSleepTime);
   NetContext* context = client->CreateNetContext(kID0);

   TestCallBack cb0;
   ServerCallBack cb1(server);

   // register receive callback function
   client->RegisterRecv(&cb0);
   server->RegisterRecv(&cb1);

   // start the net background thread
   thread* t0 = new thread(Start, client);
   thread* t1 = new thread(Start, server);
   usleep(kSleepTime);

   /*
    * node 0 sends msg to node 1
    */
   context->Send(kID0.c_str(), kID0.length());

   // free the resources
   usleep(kSleepTime);
   client->Stop();
   t0->join();
   delete client;

   usleep(kSleepTime);
   server->Stop();
   t1->join();
   delete server;
   usleep(kSleepTime);
 }

// test putting two value from node0 to node1 and node2
TEST(NetTest, CreateContexts) {
  ustore::SetStderrLogging(ustore::WARNING);

  vector<node_id_t> nodes = {kID0, kID1, kID2};

#ifdef USE_RDMA
  Net* net0 = new RdmaNet(kID0);
  Net* net1 = new RdmaNet(kID1);
  Net* net2 = new RdmaNet(kID2);
#else
  Net* net0 = new ServerZmqNet(kID0);
  Net* net1 = new ServerZmqNet(kID1);
  Net* net2 = new ServerZmqNet(kID2);
  Net* client = new ClientZmqNet();
#endif

  usleep(kSleepTime);
#ifdef USE_RDMA
  net0->CreateNetContexts(nodes);

  NetContext* ctx0_1 = net0->GetNetContext(kID1);
  NetContext* ctx0_2 = net0->GetNetContext(kID2);
  /*
   * create NetContext from node 1
   * actually this is not necessary since node 0 already connected to node 1
   * but it is ok to do this connection again (will be ignored)
   */
  net1->CreateNetContexts(nodes);

  NetContext* ctx1_0 = net1->GetNetContext(kID0);
  NetContext* ctx1_2 = net1->GetNetContext(kID2);

  // create the NetContext from node 2
  net2->CreateNetContexts(nodes);

  NetContext* ctx2_0 = net2->GetNetContext(kID0);
  NetContext* ctx2_1 = net2->GetNetContext(kID1);
#else
  client->CreateNetContexts(nodes);
  NetContext *ctx1 = client->GetNetContext(kID0);
  NetContext *ctx2 = client->GetNetContext(kID1);
  NetContext *ctx3 = client->GetNetContext(kID2);
#endif

  TestCallBack cb0;
  TestCallBack cb1;
  TestCallBack cb2;

  // register receive callback function
  net0->RegisterRecv(&cb0);
  net1->RegisterRecv(&cb1);
  net2->RegisterRecv(&cb2);

  usleep(kSleepTime);


  // start the net background thread
  thread* t0 = new thread(Start, net0);
  thread* t1 = new thread(Start, net1);
  thread* t2 = new thread(Start, net2);

#ifndef USE_RDMA
  thread* t3 = new thread(Start, client);
#endif

#ifdef USE_RDMA
  /*
   * node 0 sends msg to node 1
   */
  ctx0_1->Send(kID0.c_str(), kID0.length());

  /*
   * node 1 sends msg to node 0
   */
  ctx1_0->Send(kID1.c_str(), kID1.length());

  /*
   * node 2 sends msg to node 0
   */
  ctx2_0->Send(kID2.c_str(), kID2.length());

  /*
   * node 2 sends msg to node 1
   */
  ctx2_1->Send(kID2.c_str(), kID2.length());
#else
  ctx1->Send(kID0.c_str(), kID0.length());

  ctx2->Send(kID1.c_str(), kID1.length());

  ctx3->Send(kID2.c_str(), kID2.length());

#endif

  // free the resources
  usleep(kSleepTime);
  net0->Stop();
  t0->join();
  delete net0;
  usleep(kSleepTime);
  net1->Stop();
  t1->join();
  delete net1;
  usleep(kSleepTime);
  net2->Stop();
  t2->join();
  delete net2;
  usleep(kSleepTime);
#ifndef USE_RDMA
  client->Stop();
  t3->join();
  delete client;
#endif
}
