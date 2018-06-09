#include "gtest/gtest.h"
#include "http_msg.h"
#include "http_client.h"

using namespace std;

TEST(HttpClientTest, HttpRequestTest) {
  string host = "google";
  string port = "80";
  string target = "/";
      
  ustore::http::HttpClient hc; 
  hc.Connect(host, port);
 
  ustore::http::Request* req = new ustore::http::Request;
  hc.Send(target, ustore::http::Verb::kGet, req);
      
  ustore::http::Response* res = new ustore::http::Response;
  hc.Receive(res);

  CHECK_EQ(res->GetRes().result_int(), 301);
  hc.Shutdown();

  delete request;
  delete res;
}

