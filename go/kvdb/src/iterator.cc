// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include "db.h"
#include "utils/logging.h"

namespace ustore_kvdb {

Iterator::Iterator() {}

Iterator::Iterator(KVDB* db, ustore::Worker* wk)
    : valid_(false), db_(db), wk_(wk) {
  //wk_->ListKeys(&keys_);
  std::string cfkey = db->GetCFName();
  //wk_->ListBranches(ustore::Slice(cfkey), &keys_);
  keys_ = wk_->GetBranchRef(ustore::Slice(cfkey));
  if (keys_!=nullptr) {
    iterator_ = keys_->begin();
    valid_ = true;
  }
}
Iterator::~Iterator() {}

void Iterator::Release() { 
  delete this; 
}

void Iterator::SetRange(const std::string& r_first, const std::string& r_last) {
  r_first_ = r_first;
  r_last_ = r_last;
  SeekToFirst();
}

bool Iterator::Valid() { return valid_; }// return keys_!= nullptr && iterator_ != keys_->end(); }

void Iterator::SeekToFirst() {
  if (valid_) {
    iterator_ = keys_->begin();
    valid_ = (iterator_ != keys_->end());
  }
}

void Iterator::SeekToLast() {
  if (valid_) {
    iterator_ = keys_->end();
    iterator_--;
    valid_ = (iterator_ != keys_->end());
  }
}

void Iterator::Seek(const std::string& key) {
  SeekToFirst();
  if (valid_) {
    for (; iterator_ != keys_->end() && (iterator_->first).ToString() < key ; iterator_++); 
    valid_ = (iterator_ != keys_->end());
  }
}

bool Iterator::Next() {
  iterator_++;
  valid_ = (iterator_!= keys_->end());
  return valid_;
}

bool Iterator::Prev() {
  if (!valid_ || (iterator_ == keys_->begin())) {
    valid_ = false;
    return false;
  }
  iterator_--;
  return valid_;
}

std::string Iterator::key() const {
  //CHECK(valid_);
  //CHECK(iterator_ != keys_->end());
  return (iterator_->first).ToString();
}

std::string Iterator::value() const {
  CHECK(valid_);
  std::string ret;
  db_->Get(key(), &ret);
  return ret; 
}

namespace {
class EmptyIterator : public Iterator {
 public:
  EmptyIterator(){};
  void SetRange(const std::string&, const std::string&) override{};
  bool Valid() override { return false; }
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Seek(const std::string&) override {}
  bool Next() override { return false; }
  bool Prev() override { return false; }
  std::string key() const override {
    CHECK(false);
    return "";
  }
  std::string value() const override {
    CHECK(false);
    return "";
  }
};
}  // namespace

Iterator* NewEmptyIterator() { return new EmptyIterator(); }

}  // namespace ustore_kvdb
