// Copyright (c) 2017 The UStore Authors.

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <limits>
#include <sstream>
#include "spec/relational.h"
#include "utils/logging.h"

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
                               const std::string& branch_name) {
  std::ifstream ifs(file_path);
  USTORE_GUARD(ifs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  Table tab;
  auto ec = GetTable(table_name, branch_name, &tab);
  ERROR_CODE_FWD(ec, kUCellNotfound, kTableNotExists);
  USTORE_GUARD(ec);
  USTORE_GUARD(tab.numElements() > 0 ?
               ErrorCode::kNotEmptyTable : ErrorCode::kOK);
  std::string line;
  if (std::getline(ifs, line)) {
    // parse table schema
    auto col_names = Utils::Tokenize(line, " \t,|");
    std::vector<std::vector<std::string>> cols;
    for (auto& name : col_names) {
      cols.emplace_back(std::vector<std::string>());
    }
    auto n_cols = cols.size();
    // convert row-based entries into column-based structure
    while (std::getline(ifs, line)) {
      boost::trim(line);
      if (line.empty()) continue;
      auto row = Utils::Tokenize(line, " \t,|");
      for (size_t i = 0; i < n_cols; ++i) {
        cols[i].push_back(std::move(row[i]));
      }
    }
    ifs.close();
    // put columns to the storeage
    for (size_t i = 0; i < n_cols; ++i) {
      USTORE_GUARD(
        PutColumn(table_name, branch_name, col_names[i], cols[i]));
    }
  }
  return ErrorCode::kOK;
}

const std::string kOutputDelimiter = "\t";

ErrorCode ColumnStore::DumpCSV(const std::string& file_path,
                               const std::string& table_name,
                               const std::string& branch_name) {
  std::ofstream ofs(file_path);
  USTORE_GUARD(ofs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  Table tab;
  auto ec = GetTable(table_name, branch_name, &tab);
  ERROR_CODE_FWD(ec, kUCellNotfound, kTableNotExists);
  USTORE_GUARD(ec);
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
  ++n_rows; // counting the schema row
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
  USTORE_GUARD(tab_rst.stat);
  *table = tab_rst.value.Map();
  return ErrorCode::kOK;
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
    return ErrorCode::kKeyNotExists;
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

}  // namespace ustore
