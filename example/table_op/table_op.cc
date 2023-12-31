// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <utility>
#include <vector>
#include "benchmark/bench_utils.h"

#include "table_op.h"

namespace ustore {
namespace example {
namespace table_op {

const std::string table = "Test";
const std::string master_branch = "master";
std::string update_ref_col = "";
std::vector<std::string> update_ref_vals;
std::string update_eff_col = "";
const std::string update_eff_val = "__updated__";
std::string aggregate_col = "";
const int val_to_count = 1;
std::string latest_branch = "";

ErrorCode TableOp::Run() {
  USTORE_GUARD(Init());
  std::cout << std::endl;
  if (args_.is_diff) {
    USTORE_GUARD(Diff(master_branch, master_branch));
    for (auto& ref_val : update_ref_vals) {
      auto dev_branch = "dev-" + ref_val;
      USTORE_GUARD(Diff(master_branch, dev_branch));
    }
  } else {
    USTORE_GUARD(Load());
    std::cout << std::endl;
    for (auto& ref_val : update_ref_vals) USTORE_GUARD(Update(ref_val));
    std::cout << std::endl;
    USTORE_GUARD(Aggregate());
  }
  std::cout << std::endl;
  return ErrorCode::kOK;
}

ErrorCode TableOp::Init() {
  // load external parameters
  update_ref_col = std::move(args_.update_ref_col);
  if (update_ref_col.empty()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Missing update-referring column" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }

  auto& file_update_ref_val = args_.update_ref_val;
  if (file_update_ref_val.empty()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Missing update-referring-value file" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }
  std::ifstream ifs(file_update_ref_val);
  if (!ifs) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Failed to open file: " << file_update_ref_val << std::endl;
    return ErrorCode::kFailedOpenFile;
  }
  for (std::string line; std::getline(ifs, line);) {
    boost::trim(line);
    if (line.empty()) continue;
    update_ref_vals.push_back(std::move(line));
  }
  ifs.close();

  update_eff_col = std::move(args_.update_eff_col);
  if (update_eff_col.empty()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Missing update-effecting column" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }

  aggregate_col = std::move(args_.aggregate_col);
  if (aggregate_col.empty()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Missing aggregating column" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }

  std::cout << "Table: " << YELLOW(table)
            << ", Update-referring column: " << YELLOW(update_ref_col)
            << ", Update-effecting column: " << YELLOW(update_eff_col)
            << ", Aggregating column: " << YELLOW(aggregate_col)
            << std::endl;
  return ErrorCode::kOK;
}

ErrorCode TableOp::DropIfExists(const std::string& t) {
  bool exists;
  USTORE_GUARD(cs_.ExistsTable(t, &exists));
  if (!exists) return ErrorCode::kOK;

  std::vector<std::string> branches;
  USTORE_GUARD(cs_.ListTableBranch(t, &branches));
  for (auto& b : branches) {
    USTORE_GUARD(cs_.DeleteTable(t, b));
  }
  return ErrorCode::kOK;
}

ErrorCode TableOp::Load() {
  USTORE_GUARD(DropIfExists(table));
  USTORE_GUARD(cs_.CreateTable(table, master_branch));
  latest_branch = master_branch;

  double elapsed_ms = 0;
  size_t bytes_inc = 0;
  auto ec = MeasureByteIncrement(&bytes_inc, [this, &elapsed_ms] {
    auto ec = ErrorCode::kUnknownOp;
    elapsed_ms = Timer::TimeMilliseconds([this, &ec] {
      // execution to be evaluated
      ec = cs_.LoadCSV(args_.file, table, master_branch);
    });
    return ec;
  });
  // screen printing
  if (ec == ErrorCode::kOK) {
    long tab_sz;
    cs_.GetTableSize(table, master_branch, &tab_sz);
    std::cout << BOLD_GREEN("[SUCCESS: Load] ")
              << tab_sz << " row" << (tab_sz > 1 ? "s" : "") << " loaded";
  } else {
    std::cout << BOLD_RED("[FAILED: Load] ")
              << "Failed to load data";
  }
  std::cout << " (in " << Utils::TimeString(elapsed_ms);
  if (bytes_inc > 0) {
    std::cout << ", " << MAGENTA("+" << Utils::StorageSizeString(bytes_inc));
  }
  std::cout << ") [branch: " << BLUE(master_branch) << "]";
  if (ec != ErrorCode::kOK) {
    std::cout << RED(" --> Error(" << ec << "): " << Utils::ToString(ec));
  }
  std::cout << std::endl;
  return ec;
}

ErrorCode TableOp::Update(const std::string& ref_val) {
  static bool verified = false;
  if (!verified) {
    USTORE_GUARD(VerifyColumn(update_ref_col));
    USTORE_GUARD(VerifyColumn(update_eff_col));
    verified = true;
  }

  const auto branch = "dev-" + ref_val;
  USTORE_GUARD(cs_.BranchTable(table, latest_branch, branch));
  latest_branch = branch;

  static RandomGenerator rand;
  const auto f_update = args_.is_random_update
                        ? [] { return rand.RandomString(100); }
                        : [] { return update_eff_val; };

  double elapsed_ms = 0;
  size_t bytes_inc = 0;
  size_t n_rows_affected;
  auto ec = MeasureByteIncrement(&bytes_inc,
  [this, &ref_val, &f_update, &branch, &n_rows_affected, &elapsed_ms] {
    auto ec = ErrorCode::kUnknownOp;
    elapsed_ms = Timer::TimeMilliseconds(
    [this, &ref_val, &f_update, &branch, &n_rows_affected, &ec] {
      // execution to be evaluated
      FuncRow r;
      r.emplace(update_eff_col, f_update);
      ec = cs_.UpdateConsecutiveRows(
        table, branch, update_ref_col, ref_val, r, &n_rows_affected);
    });
    return ec;
  });
  // screen printing
  if (ec == ErrorCode::kOK) {
    std::cout << BOLD_GREEN("[SUCCESS: Update] ")
              << n_rows_affected << " row"
              << (n_rows_affected > 1 ? "s" : "") << " updated";
  } else {
    std::cout << BOLD_RED("[FAILED: Update] ")
              << "Failed to update data";
  }
  std::cout << " (in " << Utils::TimeString(elapsed_ms);
  if (bytes_inc > 0) {
    std::cout << ", " << MAGENTA("+" << Utils::StorageSizeString(bytes_inc));
  }
  std::cout << ") [branch: " << BLUE(branch) << "]";
  if (ec != ErrorCode::kOK) {
    std::cout << RED(" --> Error(" << ec << "): " << Utils::ToString(ec));
  }
  std::cout << std::endl;
  return ec;
}

ErrorCode TableOp::Diff(const std::string& lhs_branch,
                        const std::string& rhs_branch) {
  auto ec = ErrorCode::kUnknownOp;
  size_t n_rows_diff = 0;
  auto elapsed_ms = Timer::TimeMilliseconds(
  [this, &lhs_branch, &rhs_branch, &ec, &n_rows_diff]() {
    // execution to be evaluated
    Column lhs_col, rhs_col;
    ec = cs_.GetColumn(table, lhs_branch, update_eff_col, &lhs_col);
    if (ec != ErrorCode::kOK) return;
    ec = cs_.GetColumn(table, rhs_branch, update_eff_col, &rhs_col);
    if (ec != ErrorCode::kOK) return;
    auto it_diff = cs_.DiffColumn(lhs_col, rhs_col);
    while (!it_diff.end()) {
      ++n_rows_diff;
      it_diff.next();
    }
  });
  // screen printing
  if (ec == ErrorCode::kOK) {
    std::cout << BOLD_GREEN("[SUCCESS: Diff] ")
              << n_rows_diff << " row" << (n_rows_diff > 1 ? "s are" : " is")
              << " different between";
  } else {
    std::cout << BOLD_RED("[FAILED: Diff] ")
              << "Failed to compare";
  }
  std::cout << " branches \"" << BLUE(lhs_branch) << "\" and \""
            << BLUE(rhs_branch) << "\" (in "
            << Utils::TimeString(elapsed_ms) << ")";
  if (ec != ErrorCode::kOK) {
    std::cout << RED(" --> Error(" << ec << "): " << Utils::ToString(ec));
  }
  std::cout << std::endl;
  return ec;
}

ErrorCode TableOp::Aggregate() {
  USTORE_GUARD(VerifyColumn(aggregate_col));

  auto ec = ErrorCode::kUnknownOp;
  size_t count(0), all;
  auto elapsed_ms = Timer::TimeMilliseconds(
  [this, &ec, &count, &all]() {
    // execution to be evaluated
    Column col;
    ec = cs_.GetColumn(table, master_branch, aggregate_col, &col);
    if (ec != ErrorCode::kOK) return;
    for (auto it = col.Scan(); !it.end(); it.next()) {
      if (std::stod(it.value().ToString()) == val_to_count) ++count;
    }
    all = col.numElements();
  });
  // screen printing
  if (ec == ErrorCode::kOK) {
    std::cout << BOLD_GREEN("[SUCCESS: Aggregate] ")
              << count << " out of " << all << " row"
              << (count > 1 ? "s are" : " is") << " equal to " << YELLOW(1)
              << " in column \"" << YELLOW(aggregate_col) << "\"";
  } else {
    std::cout << BOLD_RED("[FAILED: Aggregate] ")
              << "Failed to perform aggregation";
  }
  std::cout << " (in " << Utils::TimeString(elapsed_ms) << ") [branch: "
            << BLUE(master_branch) << "]";
  if (ec != ErrorCode::kOK) {
    std::cout << RED(" --> Error(" << ec << "): " << Utils::ToString(ec));
  }
  std::cout << std::endl;
  return ec;
}

ErrorCode TableOp::VerifyColumn(const std::string& col) {
  static Row schema;
  if (schema.empty()) {
    USTORE_GUARD(cs_.GetTableSchema(table, master_branch, &schema));
  }
  if (schema.find(col) == schema.end()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Column \"" << col << "\" does not exist in table \""
              << table << "\"" << std::endl;
    return ErrorCode::kInvalidParameter;
  }
  return ErrorCode::kOK;
}

ErrorCode TableOp::MeasureByteIncrement(size_t* bytes_inc,
                                        const std::function<ErrorCode()>& f) {
  size_t start_bytes;
  USTORE_GUARD(cs_.GetStorageBytes(&start_bytes));

  USTORE_GUARD(f());

  size_t end_bytes;
  USTORE_GUARD(cs_.GetStorageBytes(&end_bytes));

  *bytes_inc = end_bytes - start_bytes;
  return ErrorCode::kOK;
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore
