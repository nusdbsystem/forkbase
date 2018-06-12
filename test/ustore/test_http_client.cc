// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"
#include "http/http_client.h"
#include "http/http_msg.h"

using namespace std;

TEST(HttpClientTest, HttpRequestTest) {
  string host = "www.google.com";
  string port = "80";
  string target = "/";

  ustore::http::HttpClient hc;
  EXPECT_EQ(true, hc.Connect(host, port));

  ustore::http::Request req(target, ustore::http::Verb::kGet);
  hc.Send(&req);

  ustore::http::Response res;
  hc.Receive(&res);

  EXPECT_EQ(200, res.code());
  hc.Shutdown();
}
