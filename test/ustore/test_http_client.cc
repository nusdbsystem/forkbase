// Copyright (c) 2017 The Ustore Authors.
#include <boost/algorithm/string/replace.hpp>
#include <string>

#include "gtest/gtest.h"
#include "cluster/worker_service.h"
#include "cluster/worker_client_service.h"
#include "utils/env.h"
#include "utils/logging.h"
#include "http/server.h"
#include "http/http_client.h"
#include "http/http_msg.h"

using namespace ustore;

const string CRLF = "\r\n";

void StartServer(HttpServer* server) {
  server->Start();
}

void setHeaders(http::Request* req) {
  req->SetHeaderField("host", "localhost");
  req->SetHeaderField("accept",
      "text/html, application/xhtml+xml, application/xml");
  req->SetHeaderField("accept_language", "en-US, en");
  req->SetHeaderField("accept_encoding", "gzip, deflate");
  req->SetHeaderField("connection", "keep-alive");
}

string PutB(const string& key, const string& value, const string& branch,
    http::HttpClient& hc) {
  http::Request req("/put", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&branch=" + branch + "&value=" + value);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string PutV(const string& key, const string& value, const string& version,
    http::HttpClient& hc) {
  http::Request req("/put", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&version=" + version + "&value=" + value);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string Get(const string& key, const string& version, http::HttpClient& hc) {
  http::Request req("/get", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&version=" + version);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string BranchV(const string& key, const string& version,
    const string& new_branch, http::HttpClient& hc) {
  http::Request req("/branch", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&version=" + version +
      "&new_branch=" + new_branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string BranchB(const string& key, const string& old_branch,
    const string& new_branch, http::HttpClient& hc) {
  http::Request req("/branch", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&old_branch=" + old_branch +
      "&new_branch=" + new_branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string Rename(const string& key, const string& old_branch,
    const string& new_branch, http::HttpClient& hc) {
  http::Request req("/rename", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&old_branch=" + old_branch +
      "&new_branch=" + new_branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string MergeBB(const string& key, const string& tgt_branch,
    const string& ref_branch, const string& value, http::HttpClient& hc) {
  http::Request req("/merge", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=mykey&ref_branch=" + ref_branch + "&tgt_branch=" +
      tgt_branch + "&value=" + value);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string MergeBV(const string& key, const string& tgt_branch,
    const string& version, const string& value, http::HttpClient& hc) {
  http::Request req("/merge", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=mykey&ref_version1=" + version + "&tgt_branch=" +
      tgt_branch + "&value=" + value);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string MergeVV(const string& key, const string& ref_version1,
    const string& ref_version2, const string& value, http::HttpClient& hc) {
  http::Request req("/merge", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=mykey&ref_version1=" + ref_version1 + "&ref_version2=" +
      ref_version2 + "&value=" + value);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string Head(const string& key, const string& branch, http::HttpClient& hc) {
  http::Request req("/head", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&branch=" + branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string IsBranchHead(const string& key, const string& version,
    const string& branch, http::HttpClient& hc) {
  http::Request req("/isbranchhead", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&version=" + version + "&branch=" + branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string Latest(const string& key, http::HttpClient& hc) {
  http::Request req("/latest", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string IsLatestVersion(const string& key, const string& version,
    http::HttpClient& hc) {
  http::Request req("/islatestversion", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&version=" + version);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string ListK(http::HttpClient& hc) {
  http::Request req("/list", http::Verb::kGet);
  setHeaders(&req);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  return data.substr(0, data.length()-1);
}

string ListB(const string& key, http::HttpClient& hc) {
  http::Request req("/list", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  return data.substr(0, data.length()-1);
}

string ExistsK(const string& key, http::HttpClient& hc) {
  http::Request req("/exists", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string ExistsB(const string& key, const string& branch, http::HttpClient& hc) {
  http::Request req("/exists", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&branch=" + branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

string Delete(const string& key, const string& branch, http::HttpClient& hc) {
  http::Request req("/delete", http::Verb::kPost);
  setHeaders(&req);
  req.SetBody("key=" + key + "&branch=" + branch);
  hc.Send(&req);
  http::Response res;
  hc.Receive(&res);
  string data = res.body();
  boost::replace_all(data, CRLF, "");
  return data;
}

TEST(HttpClientTest, HttpRequestTest) {
  int port = Env::Instance()->config().http_port();
  // launch workers
  Env::Instance()->m_config().set_worker_file("conf/test_single_worker.lst");
  std::ifstream fin(Env::Instance()->config().worker_file());
  string worker_addr;
  std::vector<ustore::WorkerService*> workers;
  while (fin >> worker_addr)
    workers.push_back(new ustore::WorkerService(worker_addr, false));

  for (auto& worker : workers) worker->Run();

  // launch clients
  ustore::WorkerClientService service;
  service.Run();
  // 1 thread
  WorkerClient client = service.CreateWorkerClient();

  // start the http server
  HttpServer server(&client, port);
  std::thread server_thread(StartServer, &server);
  sleep(1);

  // connect to the http server
  http::HttpClient hc;
  if (!hc.Connect("localhost", std::to_string(port))) {
    DLOG(INFO) << "cannot connect to the server";
    return;
  }

  string key = "mykey";
  string value1 = "value1";
  string branch1 = "mybranch1";

  // put a new key value
  string version = PutB(key, value1, branch1, hc);
  string version1 = version;
  DLOG(INFO) << "Got version: " << version;

  // get the value
  string value = Get(key, version, hc);
  DLOG(INFO) << "Got value: " << value;
  CHECK_EQ(value1, value);

  // put a key value based on the previous version
  string value2 = "value2";
  version = PutV(key, value2, version, hc);
  string version2 = version;
  DLOG(INFO) << "Got version: " << version;

  // get the value
  value = Get(key, version2, hc);
  DLOG(INFO) << "Got value: " << value;
  CHECK_EQ(value, value2);

  // check if a key exists
  string status = ExistsK(key, hc);
  CHECK_EQ(status, "true");
  status = ExistsK(key + "error", hc);
  CHECK_EQ(status, "false");

  // check if a key and branch exists
  status = ExistsB(key, branch1, hc);
  CHECK_EQ(status, "true");
  status = ExistsB(key + "error", branch1, hc);
  CHECK_EQ(status, "false");
  status = ExistsB(key, branch1 + "error", hc);
  CHECK_EQ(status, "false");
  status = ExistsB(key + "error", branch1 + "error", hc);
  CHECK_EQ(status, "false");

  // head of the branch
  version = Head(key, branch1, hc);
  CHECK_EQ(version, version1);

  // check if it is the branch head
  status = IsBranchHead(key, version, branch1, hc);
  CHECK_EQ(status, "true");
  status = IsBranchHead(key, version2, branch1, hc);
  CHECK_EQ(status, "false");

  // latest version of the key
  version = Latest(key, hc);
  CHECK_EQ(version, version2);

  // check if a key is latest
  status = IsLatestVersion(key, version, hc);
  CHECK_EQ(status, "true");
  status = IsLatestVersion(key, version1, hc);
  CHECK_EQ(status, "false");

  // put a new key
  string key2 = "mykey2";
  version = PutB(key2, value1, branch1, hc);
  DLOG(INFO) << "Got version: " << version;

  // list the keys
  string keys_str = ListK(hc);
  auto keys = Utils::Tokenize(keys_str, CRLF.c_str());
  std::sort(keys.begin(), keys.end());
  std::vector<string> expected_keys = {key2, key};
  std::sort(expected_keys.begin(), expected_keys.end());
  EXPECT_EQ(expected_keys, keys);

  // branch based on version
  string branch2 = "mybranch2";
  status = BranchV(key, version2, branch2, hc);
  DLOG(INFO) << "New branch " << branch2 << ": " << status;
  CHECK(status == "OK" || status == "Branch Error: 5");

  // list the branches
  string branches_str = ListB(key, hc);
  auto branches = Utils::Tokenize(branches_str, CRLF.c_str());
  std::sort(branches.begin(), branches.end());
  std::vector<string> expected_branches = {branch1, branch2};
  std::sort(expected_branches.begin(), expected_branches.end());
  EXPECT_EQ(expected_branches, branches);

  // branch based on branch
  string branch3 = "mybranch3";
  status = BranchB(key, branch1, branch3, hc);
  DLOG(INFO) << "New branch " << branch3 << ": " << status;
  CHECK(status == "OK" || status == "Branch Error: 5");

  // rename a branch
  string branch4 = "mybranch4";
  status = Rename(key, branch1, branch4, hc);
  DLOG(INFO) << "Rename branch from " <<
             branch1 << " to " << branch4 << ": " << status;
  CHECK(status == "OK" || status == "Rename Error: 5");

  // merge two values
  // merge target branch to a referring branch
  string value3 = "value3";
  version = MergeBB(key, branch2, branch3, value3, hc);
  DLOG(INFO) << "Merge branch "
             << branch3 << " based on " << branch2 << ": " << version;

  // get back the value to check
  value = Get(key, version, hc);
  CHECK_EQ(value, value3);

  // merge target branch to a referring version
  string value4 = "value4";
  string version3 = MergeBV(key, branch2, version, value4, hc);
  DLOG(INFO) << "Merge branch "
             << branch2 << " based on " << version << ": " << version3;

  // get back the value to check
  value = Get(key, version3, hc);
  CHECK_EQ(value, value4);

  // merge two existing versions
  string value5 = "value5";
  string version4 = MergeVV(key, version1, version2, value5, hc);
  DLOG(INFO) << "Merge version "
             << version1 << " based on " << version2 << ": " << version4;

  // get back the value to check
  value = Get(key, version4, hc);
  CHECK_EQ(value, value5);

  // delete a branch
  status = ExistsB(key, branch4, hc);
  CHECK_EQ(status, "true");
  status = Delete(key, branch4, hc);
  CHECK_EQ(status, "OK");
  status = ExistsB(key, branch4, hc);
  CHECK_EQ(status, "false");

  hc.Shutdown();
  server.Stop();
  sleep(1);
  server_thread.join();

  // stop the client service
  service.Stop();
  // stop workers
  for (auto& p : workers) delete p;

}
