// Copyright (c) 2017 The Ustore Authors.

#include <stdlib.h>
#include <chrono>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

#include "cluster/remote_client_service.h"
#include "spec/relational.h"

namespace ustore {
namespace example {
namespace huawei {

constexpr int kInitForMs = 75;

std::vector<std::string> CreateIntColumn(size_t num_records, int max) {
  std::vector<std::string> result;

  for (size_t i = 0; i < num_records; ++i) {
    int r = rand() % max;
    std::stringstream stream;
    stream << r;
    result.push_back(stream.str());
  }
  return result;
}

static const char alphabet[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";

std::vector<std::string> CreateStrColumn(size_t num_records, int length) {
  std::vector<std::string> result;
  for (size_t i = 0; i < num_records; ++i) {
    std::string str;
    std::generate_n(std::back_inserter(str), length, [&]() {
      int idx = rand() % 62;
      return alphabet[idx]; });
    result.push_back(str);
  }
  return result;
}

std::vector<std::string> CreateLocationColumn(size_t num_records) {
  std::vector<std::string> result;
  for (size_t i = 0; i < num_records; ++i) {
    std::stringstream sstr;
    sstr << "("
         << rand() % 90 << "." << rand() % 100 << "#"
         << rand() % 180 << "." << rand() % 100
         << ")";
    result.push_back(sstr.str());
  }
  return result;
}

std::vector<std::string> CreateFloatColumn(size_t num_records) {
  std::vector<std::string> result;
  for (size_t i = 0; i < num_records; ++i) {
    std::stringstream sstr;
    sstr << ""
         << rand() % 100 << "." << rand() % 100
         << "";
    result.push_back(sstr.str());
  }
  return result;
}

std::vector<std::string> CreateTimeColumn(size_t num_records) {
  std::vector<std::string> result;
  for (size_t i = 0; i < num_records; ++i) {
    std::stringstream sstr;
    sstr << "["
         << rand() % 24 << ":" << rand() % 60
         << "]";
    result.push_back(sstr.str());
  }
  return result;
}


int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  // connect to UStore servcie
  RemoteClientService ustore_svc("");
  ustore_svc.Init();
  std::thread ustore_svc_thread(&RemoteClientService::Start, &ustore_svc);
  std::this_thread::sleep_for(std::chrono::milliseconds(75));
  ClientDb client_db = ustore_svc.CreateClientDb();
  ColumnStore* cs = new ColumnStore(&client_db);

  size_t num_records = 1000;
  std::string table_name("TB_LOCATION");
  std::string branch_name("master");
  cs->CreateTable(table_name, branch_name);

  std::vector<std::string> cols;
  std::vector<std::vector<std::string>> vals;
  cols.push_back("MSISDN");
  vals.push_back(CreateStrColumn(num_records, 15));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("IMSI");
  vals.push_back(CreateStrColumn(num_records, 15));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("IMEI");
  vals.push_back(CreateStrColumn(num_records, 15));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("HOMEAREA");
  vals.push_back(CreateStrColumn(num_records, 64));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("CURAREA");
  vals.push_back(CreateStrColumn(num_records, 64));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("LOCATION");
  vals.push_back(CreateLocationColumn(num_records));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());
  cols.push_back("CAPTURETIME");
  vals.push_back(CreateIntColumn(num_records, 1000000000));
  cs->PutColumn(table_name, branch_name, cols.back(), vals.back());

  ustore_svc.Stop();
  ustore_svc_thread.join();
  delete cs;
  std::cout << "Table TB_LOCATION Loaded" << std::endl;

  std::ofstream os("hbase.csv");
  for (size_t i = 0; i < cols.size(); ++i) {
    if (i) os << ",";
    os << cols[i];
  }
  os << std::endl;
  for (size_t k = 0; k < vals[0].size(); ++k) {
    for (size_t i = 0; i < vals.size(); ++i) {
      if (i) os << ",";
      os << vals[i][k];
    }
    os << std::endl;
  }
  std::cout << "hbase.csv generated" << std::endl;

  return 0;
}

}  // namespace huawei
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::huawei::main(argc, argv);
}
