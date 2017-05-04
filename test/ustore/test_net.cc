/*
 * test_rdma.cc
 *
 *  Created on: Jul 26, 2016
 *      Author: zhanghao
 */

#include "gtest/gtest.h"
#include <gflags/gflags.h>
#include <unistd.h>
#include <thread>
#include <assert.h>
#include <vector>
#include "net/net.h"
#include "utils/logging.h"

//by default, it choose the zmq_net
//if u want to test the rdma net, you have to define USE_RDMA
//#define USE_RDMA

#ifdef USE_RDMA
#include "net/rdma_net.h"
#else
#include "net/zmq_net.h"
#endif

using namespace ustore;
using std::string;
using std::thread;
using std::vector;

class TestCallBack : public CallBack {
 public:
  TestCallBack(void* handler = nullptr): CallBack(handler) {};
  void operator()(const void *msg, int size, const node_id_t& source) {
    ((char*) msg)[size] = '\0';
    EXPECT_TRUE(!strcmp(static_cast<const char*>(msg),
                static_cast<const char*>(source.c_str())));
    LOG(WARNING) << "msg = " << static_cast<const char*>(msg);
  }
};

void Start(Net* net) {
  net->Start();
}

string id0 = "localhost:1235";
string id1 = "localhost:1236";
string id2 = "localhost:1237";

// test putting two value from node0 to node1 and node2
TEST(NetTest, MsgTest) {
  ustore::SetStderrLogging(ustore::WARNING);

#ifdef USE_RDMA
  Net* net0 = new RdmaNet(id0);
  Net* net1 = new RdmaNet(id1);
  Net* net2 = new RdmaNet(id2);
#else
  Net* net0 = new ZmqNet(id0);
  Net* net1 = new ZmqNet(id1);
  Net* net2 = new ZmqNet(id2);
#endif

  sleep(1.0);
  NetContext* ctx0_1 = net0->CreateNetContext(id1);
  NetContext* ctx0_2 = net0->CreateNetContext(id2);

  /*
   * create NetContext from node 1
   * actually this is not necessary since node 0 already connected to node 1
   * but it is ok to do this connection again (will be ignored)
   */
  NetContext* ctx1_0 = net1->CreateNetContext(id0);
  NetContext* ctx1_2 = net1->CreateNetContext(id2);

  //create the NetContext from node 2
  NetContext* ctx2_0 = net2->CreateNetContext(id0);
  NetContext* ctx2_1 = net2->CreateNetContext(id1);

  TestCallBack cb0;
  TestCallBack cb1;
  TestCallBack cb2;

  //register receive callback function
  net0->RegisterRecv(&cb0);
  net1->RegisterRecv(&cb1);
  net2->RegisterRecv(&cb2);

  sleep(1.0);

  //start the net background thread
  thread* t0 = new thread(Start, net0);
  thread* t1 = new thread(Start, net1);
  thread* t2 = new thread(Start, net2);

  /*
   * node 0 sends msg to node 1
   */
  ctx0_1->Send(id0.c_str(), id0.length());

  /*
   * node 1 sends msg to node 0
   */
  ctx1_0->Send(id1.c_str(), id1.length());

  /*
   * node 2 sends msg to node 0
   */
  ctx2_0->Send(id2.c_str(), id2.length());

  /*
   * node 2 sends msg to node 1
   */
  ctx2_1->Send(id2.c_str(), id2.length());

  //free the resources
  sleep(1);
  net0->Stop();
  net1->Stop();
  net2->Stop();
  sleep(1);
  t0->join();
  t1->join();
  t2->join();
  delete net0;
  delete net1;
  delete net2;
}

// test putting two value from node0 to node1 and node2
TEST(NetTest, CreateContexts) {
  ustore::SetStderrLogging(ustore::WARNING);

  vector<node_id_t> nodes = {id0, id1, id2};

#ifdef USE_RDMA
  Net* net0 = new RdmaNet(id0);
  Net* net1 = new RdmaNet(id1);
  Net* net2 = new RdmaNet(id2);
#else
  Net* net0 = new ZmqNet(id0);
  Net* net1 = new ZmqNet(id1);
  Net* net2 = new ZmqNet(id2);
#endif

  sleep(1.0);
  net0->CreateNetContexts(nodes);
  NetContext* ctx0_1 = net0->GetNetContext(id1);
  NetContext* ctx0_2 = net0->GetNetContext(id2);

  /*
   * create NetContext from node 1
   * actually this is not necessary since node 0 already connected to node 1
   * but it is ok to do this connection again (will be ignored)
   */
  net1->CreateNetContexts(nodes);
  NetContext* ctx1_0 = net1->GetNetContext(id0);
  NetContext* ctx1_2 = net1->GetNetContext(id2);

  //create the NetContext from node 2
  net2->CreateNetContexts(nodes);
  NetContext* ctx2_0 = net2->GetNetContext(id0);
  NetContext* ctx2_1 = net2->GetNetContext(id1);

  TestCallBack cb0;
  TestCallBack cb1;
  TestCallBack cb2;

  //register receive callback function
  net0->RegisterRecv(&cb0);
  net1->RegisterRecv(&cb1);
  net2->RegisterRecv(&cb2);

  sleep(1.0);

  //start the net background thread
  thread* t0 = new thread(Start, net0);
  thread* t1 = new thread(Start, net1);
  thread* t2 = new thread(Start, net2);

  /*
   * node 0 sends msg to node 1
   */
  ctx0_1->Send(id0.c_str(), id0.length());

  /*
   * node 1 sends msg to node 0
   */
  ctx1_0->Send(id1.c_str(), id1.length());

  /*
   * node 2 sends msg to node 0
   */
  ctx2_0->Send(id2.c_str(), id2.length());

  /*
   * node 2 sends msg to node 1
   */
  ctx2_1->Send(id2.c_str(), id2.length());

  //free the resources
  sleep(1);
  net0->Stop();
  net1->Stop();
  net2->Stop();
  sleep(1);
  delete net0;
  delete net1;
  delete net2;
}
