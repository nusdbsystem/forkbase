#include <utility>
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "utils/logging.h"
#include "worker/worker.h"

#include "config.h"
#include "simple_dataset.h"
#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

const auto data =
  std::move(Utils::ToStringMap(SimpleDataset::GenerateTable()));

Worker worker(Config::kWorkID);
const Slice branch_master("master");
const Slice branch_a("analytics-A");
const Slice branch_b("analytics-B");

void LoadDataset() {
  Hash version;
  ErrorCode ec;
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    const auto col_value = Value(Slice(cv.second));
    ec = worker.Put(col_name, col_value, branch_master, &version);
    CHECK(ec == ErrorCode::kOK);
  }
}

void ScanBranchMaster() {
  ErrorCode ec;
  Value col_value;
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    ec = worker.Get(col_name, branch_master, &col_value);
    CHECK(col_value.type() == UType::kString);
    CHECK_EQ(cv.second, col_value.slice());
  }
}

void PerformAnalyticsA() {
  std::map<Slice, std::string> data_a;
  Value col_value;
  ErrorCode ec;
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    ec = worker.Branch(col_name, branch_master, branch_a);
    CHECK(ec == ErrorCode::kOK);
    ec = worker.Get(col_name, branch_a, &col_value);
    CHECK(ec == ErrorCode::kOK);
    CHECK(col_value.type() == UType::kString);
    CHECK_EQ(cv.second, col_value.slice());
    data_a[col_name] = std::move(col_value.slice().to_string());
  }

  static const std::string rst_col_name("ana-A");
  Hash version;
  ec = worker.Put(Slice(rst_col_name), Value(Slice(Utils::ToString(
                    SimpleDataset::GenerateColumn(rst_col_name)))),
                  branch_a, &version);
  CHECK(ec == ErrorCode::kOK);
}

void ScanAnalyticsA() {
  ErrorCode ec;
  Value col_value;
  ec = worker.Get(Slice("ana-A"), branch_a, &col_value);
  std::cout << "ana-A: " << col_value << std::endl;
}

void PerformAnalyticsB() {}

static int main(int argc, char* argv[]) {
  LoadDataset();
  ScanBranchMaster();
  PerformAnalyticsA();
  ScanAnalyticsA();
  PerformAnalyticsB();
  return 0;
}

} // namespace ca
} // namespace example
} // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
