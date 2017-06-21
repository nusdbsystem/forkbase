// Copyright (c) 2017 The UStore Authors.

#include <fstream>
#include <future>
#include <iomanip>
#include <limits>
#include "cluster/remote_client_service.h"
#include "utils/logging.h"
#include "utils/service_context.h"
#include "utils/sync_task_line.h"
#include "spec/relational.h"

namespace ustore {

ErrorCode ColumnStore::ExistsTable(const std::string& table_name,
                                   bool* exist) {
  auto rst = odb_.ListBranches(Slice(table_name));
  *exist = !rst.value.empty();
  return rst.stat;
}

ErrorCode ColumnStore::ExistsTable(const std::string& table_name,
                                   const std::string& branch_name,
                                   bool* exist) {
  auto rst = odb_.Exists(Slice(table_name), Slice(branch_name));
  *exist = rst.value;
  return rst.stat;
}

ErrorCode ColumnStore::CreateTable(const std::string& table_name,
                                   const std::string& branch_name) {
  auto rst = odb_.Exists(Slice(table_name), Slice(branch_name));
  auto& tab_exist = rst.value;
  return tab_exist ? ErrorCode::kBranchExists :
         odb_.Put(Slice(table_name), Table(), Slice(branch_name)).stat;
}

ErrorCode ColumnStore::LoadCSV(const std::string& file_path,
                               const std::string& table_name,
                               const std::string& branch_name,
                               size_t batch_size, bool print_progress) {
  Table tab;
  USTORE_GUARD(GetTable(table_name, branch_name, &tab));
  USTORE_GUARD(tab.numElements() > 0 ?
               ErrorCode::kNotEmptyTable : ErrorCode::kOK);
  std::ifstream ifs(file_path);
  auto ec = ifs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile;
  USTORE_GUARD(ec);
  std::string line;
  std::vector<std::string> col_names;
  if (std::getline(ifs, line)) {
    // parse table schema
    col_names = Utils::Tokenize(line, " \t,|");
    // initialization
    for (auto& name : col_names) {
      auto key = GlobalKey(table_name, name);
      static const Column empty_col(std::vector<Slice>({}));
      ec = odb_.Put(Slice(key), empty_col, Slice(branch_name)).stat;
      if (ec != ErrorCode::kOK) break;
    }
  }
  if (ec == ErrorCode::kOK) {
    ec = LoadCSV(ifs, table_name, branch_name, col_names, batch_size,
                 print_progress);
  }
  ifs.close();
  return ec;
}

ErrorCode ColumnStore::LoadCSV(std::ifstream& ifs,
                               const std::string& table_name,
                               const std::string& branch_name,
                               const std::vector<std::string>& col_names,
                               size_t batch_size, bool print_progress) {
  BlockingQueue<std::vector<std::vector<std::string>>> batch_queue(2);
  auto stat_flush = ErrorCode::kOK;
  // launch another thread for data flushing
  auto thread_flush = std::async(std::launch::async, [&]() {
    FlushCSV(table_name, branch_name, col_names, batch_queue, stat_flush,
             print_progress);
  });
  // this thread shards the input data
  ShardCSV(ifs, batch_size, col_names.size(), batch_queue, stat_flush);
  thread_flush.wait();
  // update table once loading columns completes with success
  auto ec = stat_flush;
  if (ec == ErrorCode::kOK) {
    Table tab;
    for (auto& name : col_names) {
      ec = GetTable(table_name, branch_name, &tab);
      if (ec != ErrorCode::kOK) break;
      auto col_key = GlobalKey(table_name, name);
      auto rst = odb_.GetBranchHead(Slice(col_key), Slice(branch_name));
      ec = rst.stat;
      if (ec != ErrorCode::kOK) break;
      auto col_ver = rst.value.ToBase32();
      tab.Set(Slice(name), Slice(col_ver));
      ec = odb_.Put(Slice(table_name), tab, Slice(branch_name)).stat;
    }
  }
  return ec;
}

void ColumnStore::ShardCSV(
  std::ifstream& ifs, size_t batch_size, size_t n_cols,
  BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
  const ErrorCode& stat_flush) {
  auto f_get_empty_batch = [&n_cols] {
    std::vector<std::vector<std::string>> cols;
    for (size_t i = 0; i < n_cols; ++i) {
      cols.emplace_back(std::vector<std::string>());
    }
    return cols;
  };
  auto cols = f_get_empty_batch();
  std::string line;
  while (std::getline(ifs, line)) {
    if (line.empty()) continue;
    auto row = Utils::Tokenize(line, " \t,|");
    for (size_t i = 0; i < n_cols; ++i) {
      cols[i].push_back(std::move(row[i]));
    }
    if (cols[0].size() >= batch_size) {
      if (stat_flush != ErrorCode::kOK) return;
      batch_queue.Put(std::move(cols));
      cols = f_get_empty_batch();
    }
  }
  // deal with the residuals and exit
  batch_queue.Put(std::move(cols));
  batch_queue.Put(f_get_empty_batch());
}

class FlushTaskLine
  : public SyncTaskLine<std::vector<std::string>, ErrorCode> {
 public:
  FlushTaskLine()
    : SyncTaskLine<DataType, ErrorCodeType>(),
      db_(GetClientDb()), odb_(&db_) {}

  ~FlushTaskLine() = default;

  ErrorCode Consume(const std::vector<std::string>& sector) override {
    // retrieve the previous column from storage
    auto col_get_rst = odb_.Get(col_key_, branch_);
    USTORE_GUARD(col_get_rst.stat);
    auto col = col_get_rst.value.List();
    // concatenate the current column to storage
    std::vector<Slice> col_slices;
    for (const auto& str : sector) col_slices.emplace_back(str);
    col.Append(col_slices);
    return odb_.Put(col_key_, col, branch_).stat;
  }

  bool Terminate(const std::vector<std::string>& sector) override {
    return sector.empty();
  }

  inline void SetColumn(const std::string& tab_name,
                        const std::string& branch_name,
                        const std::string& col_name) {
    col_key_str_ = ColumnStore::GlobalKey(tab_name, col_name);
    col_key_ = Slice(col_key_str_);
    branch_str_ = branch_name;
    branch_ = Slice(branch_str_);
  }

 private:
  using DataType = std::vector<std::string>;
  using ErrorCodeType = ErrorCode;

  static ClientDb GetClientDb() {
    static ServiceContext svc_ctx;
    return svc_ctx.GetClientDb();
  }

  ClientDb db_;
  ObjectDB odb_;

  std::string col_key_str_;
  Slice col_key_;
  std::string branch_str_;
  Slice branch_;
};

void ColumnStore::FlushCSV(
  const std::string& table_name, const std::string& branch_name,
  const std::vector<std::string>& col_names,
  BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
  ErrorCode& stat, bool print_progress) {
  auto n_cols = col_names.size();
  // launch the sector-flushing threads
  FlushTaskLine task_lines[n_cols];
  std::thread threads[n_cols];
  for (size_t i = 0; i < n_cols; ++i) {
    auto& tl = task_lines[i];
    tl.SetColumn(table_name, branch_name, col_names[i]);
    threads[i] = tl.Launch();
  }
  // load data into storage by batches in parallel
  auto f_current_flush = [&n_cols, &task_lines](
  const std::vector<std::vector<std::string>>& cols) {
    const int load_size = cols[0].size();
    // dispatch task to the task lines
    for (size_t i = 0; i < n_cols; ++i) {
      task_lines[i].Produce(std::move(cols[i]));
    }
    // barrier: wait for all talks lines to complete
    for (size_t i = 0; i < n_cols; ++i) {
      auto ec = task_lines[i].Sync();
      if (ec != ErrorCode::kOK) return -static_cast<int>(ec);
    }
    return load_size;
  };
  // flush data batches iteratively
  size_t cnt_loaded = 0;
  while (true) {
    auto cols = batch_queue.Take();
    // flush data batch into storage
    const int num_loaded = f_current_flush(cols);
    if (num_loaded == 0) break;
    if (num_loaded < 0) {
      stat = static_cast<ErrorCode>(-num_loaded);
      static std::vector<std::string> empty_sector;
      for (auto& tl : task_lines) tl.Produce(empty_sector);
      break;
    }
    // print progress
    if (print_progress && num_loaded > 0) {
      cnt_loaded += num_loaded;
      std::cout << GREEN("[FLUSHED] ")
                << "Number of rows loaded into storage: " << cnt_loaded
                << std::endl;
    }
  }
  // barrier: wait for the sector-flushing threads to complete
  for (auto& t : threads) t.join();
}

const char kOutputDelimiter[] = "|";

ErrorCode ColumnStore::DumpCSV(const std::string& file_path,
                               const std::string& table_name,
                               const std::string& branch_name) {
  std::ofstream ofs(file_path);
  USTORE_GUARD(ofs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  USTORE_GUARD(tab.numElements() == 0 ?
               ErrorCode::kEmptyTable : ErrorCode::kOK);
  // retrieve the column-based table
  std::vector<std::vector<std::string>> cols;
  auto n_rows = std::numeric_limits<size_t>::max();
  for (auto it_tab = tab.Scan(); !it_tab.end(); it_tab.next()) {
    auto col_name = it_tab.key().ToString();
    Column col;
    USTORE_GUARD(
      GetColumn(table_name, branch_name, col_name, &col));
    std::vector<std::string> col_str = { std::move(col_name) };
    for (auto it_col = col.Scan(); !it_col.end(); it_col.next()) {
      col_str.emplace_back(it_col.value().ToString());
    }
    n_rows = std::min(n_rows, col.numElements());
    cols.push_back(std::move(col_str));
  }
  ++n_rows;  // counting the schema row
  // write the column-based table to the row-based CSV file
  for (size_t i = 0; i < n_rows; ++i) {
    ofs << cols[0][i];
    for (size_t j = 1; j < cols.size(); ++j) {
      ofs << kOutputDelimiter << cols[j][i];
    }
    ofs << std::endl;
  }
  ofs.close();
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::GetTable(const std::string& table_name,
                                const std::string& branch_name,
                                Table* table) {
  auto tab_rst = odb_.Get(Slice(table_name), Slice(branch_name));
  auto& ec = tab_rst.stat;
  if (ec == ErrorCode::kOK) {
    *table = tab_rst.value.Map();
  } else {
    ERROR_CODE_FWD(ec, kKeyNotExists, kTableNotExists);
  }
  return ec;
}

ErrorCode ColumnStore::BranchTable(const std::string& table_name,
                                   const std::string& old_branch_name,
                                   const std::string& new_branch_name) {
  const Slice old_branch(old_branch_name);
  const Slice new_branch(new_branch_name);
  // branch all columns of the table
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, old_branch_name, &tab));
  for (auto it = tab.Scan(); !it.end(); it.next()) {
    auto col_key = GlobalKey(table_name, it.key());
    USTORE_GUARD(
      odb_.Branch(Slice(col_key), old_branch, new_branch));
  }
  // branch the table
  return odb_.Branch(Slice(table_name), old_branch, new_branch);
}

ErrorCode ColumnStore::ListTableBranch(const std::string& table_name,
                                       std::vector<std::string>* branches) {
  auto rst = odb_.ListBranches(Slice(table_name));
  USTORE_GUARD(rst.stat);
  *branches = std::move(rst.value);
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::MergeTable(const std::string& table_name,
                                  const std::string& tgt_branch_name,
                                  const std::string& ref_branch_name,
                                  const std::string& remove_col_name) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, tgt_branch_name, &tab));
  tab.Remove(Slice(remove_col_name));
  return odb_.Merge(Slice(table_name), tab, Slice(tgt_branch_name),
                    Slice(ref_branch_name)).stat;
}

ErrorCode ColumnStore::MergeTable(
  const std::string& table_name, const std::string& tgt_branch_name,
  const std::string& ref_branch_name, const std::string& new_col_name,
  const std::vector<std::string>& new_col_vals) {
  Version new_col_ver;
  USTORE_GUARD(
    WriteColumn(table_name, tgt_branch_name, new_col_name, new_col_vals,
                &new_col_ver));
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, tgt_branch_name, &tab));
  tab.Set(Slice(new_col_name), Slice(new_col_ver));
  return odb_.Merge(Slice(table_name), tab, Slice(tgt_branch_name),
                    Slice(ref_branch_name)).stat;
}

ErrorCode ColumnStore::DeleteTable(const std::string& table_name,
                                   const std::string& branch_name) {
  // delete all columns of the table
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  for (auto it = tab.Scan(); !it.end(); it.next()) {
    auto col_key = GlobalKey(table_name, it.key());
    USTORE_GUARD(
      odb_.Delete(Slice(col_key), Slice(branch_name)));
  }
  // delete the table
  return odb_.Delete(Slice(table_name), Slice(branch_name));
}

ErrorCode ColumnStore::ExistsColumn(const std::string& table_name,
                                    const std::string& col_name,
                                    bool* exist) {
  auto col_key = GlobalKey(table_name, col_name);
  auto rst = odb_.ListBranches(Slice(col_key));
  *exist = !rst.value.empty();
  return rst.stat;
}

ErrorCode ColumnStore::ExistsColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name,
                                    bool* exist) {
  auto col_key = GlobalKey(table_name, col_name);
  auto rst = odb_.Exists(Slice(col_key), Slice(branch_name));
  *exist = rst.value;
  return rst.stat;
}

ErrorCode ColumnStore::GetColumn(
  const std::string& table_name, const std::string& branch_name,
  const std::string& col_name, Column* col) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  if (tab.Get(Slice(col_name)).empty()) {
    LOG(WARNING) << "Column \"" << col_name << "\" does not exist in Table \""
                 << table_name << "\" of Branch \"" << branch_name << "\"";
    return ErrorCode::kColumnNotExists;
  }
  return ReadColumn(table_name, branch_name, col_name, col);
}

ErrorCode ColumnStore::PutColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name,
                                 const std::vector<std::string>& col_vals) {
  Version col_ver;
  USTORE_GUARD(
    WriteColumn(table_name, branch_name, col_name, col_vals, &col_ver));
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  tab.Set(Slice(col_name), Slice(col_ver));
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).stat;
}

ErrorCode ColumnStore::ListColumnBranch(const std::string& table_name,
                                        const std::string& col_name,
                                        std::vector<std::string>* branches) {
  auto col_key = GlobalKey(table_name, col_name);
  auto rst = odb_.ListBranches(Slice(col_key));
  USTORE_GUARD(rst.stat);
  *branches = std::move(rst.value);
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::DeleteColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name) {
  // delele column in the storage
  auto col_key = GlobalKey(table_name, col_name);
  USTORE_GUARD(
    odb_.Delete(Slice(col_key), Slice(branch_name)));
  // delete the column entry in the table
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  tab.Remove(Slice(col_name));
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).stat;
}

ErrorCode ColumnStore::GetRow(const std::string& table_name,
                              const std::string& branch_name,
                              const std::string& ref_col_name,
                              const std::string& ref_val,
                              std::unordered_map<size_t, Row>* rows) {
  rows->clear();
  Column ref_col;
  USTORE_GUARD(
    GetColumn(table_name, branch_name, ref_col_name, &ref_col));
  // search for row indices
  for (auto it = ref_col.Scan(); !it.end(); it.next()) {
    if (it.value() == ref_val) {
      Row r;
      r.emplace(ref_col_name, ref_val);
      rows->emplace(it.index(), std::move(r));
    }
  }
  if (rows->empty()) return ErrorCode::kRowNotExists;
  // construct rows according to the indices
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  for (auto it = tab.Scan(); !it.end(); it.next()) {
    auto col_name = it.key().ToString();
    if (col_name == ref_col_name) continue;
    Column col;
    USTORE_GUARD(
      ReadColumn(table_name, branch_name, col_name, &col));
    for (auto i_r : *rows) {
      rows->at(i_r.first).emplace(col_name, col.Get(i_r.first).ToString());
    }
  }
  return ErrorCode::kOK;
}

}  // namespace ustore
