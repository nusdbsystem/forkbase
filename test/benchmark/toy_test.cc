// Copyright (c) 2017 The Ustore Authors.

// Please add your header file here
#include <cstring>
#include <string>
#include <sstream>
#include <utility>

#include "worker/worker.h"
#include "spec/object_db.h"
#include "types/client/vset.h"
#include "types/client/vmap.h"

// Please add namespace here
// The following line is an example
using namespace ustore;

int main(int argc, char* argv[]) {
  Worker worker {2018, nullptr, false};
  ObjectDB db(&worker);

// // Init a empty set
//   VSet vset;
//   Slice key("key");
//   Slice branch("branch");
//   auto put1_rsp = db.Put(key, vset, branch);
//   CHECK_EQ(put1_rsp.stat, ErrorCode::kOK);

// // Read out that empty set
//   auto  get1_rsp = db.Get(key, branch);
//   CHECK_EQ(get1_rsp.stat, ErrorCode::kOK);
//   VSet vset1 = get1_rsp.value.Set();
//   CHECK_EQ(vset1.numElements(), 0);


// // Insert a key
//   Slice ele_key("ele_key");
//   vset1.Set(ele_key);
//   auto put2_rsp = db.Put(key, vset1, branch);
//   CHECK_EQ(put2_rsp.stat, ErrorCode::kOK);


// // Read out the previous key
//   auto  get2_rsp = db.Get(key, branch);
//   CHECK_EQ(get2_rsp.stat, ErrorCode::kOK);
//   VSet vset2 = get2_rsp.value.Set();
//   CHECK_EQ(vset2.numElements(), 1);



// // Insert the same key
//   Slice ele_key1("ele_key");
//   vset2.Set(ele_key1);
//   auto put3_rsp = db.Put(key, vset2, branch);
//   CHECK_EQ(put3_rsp.stat, ErrorCode::kOK);


  Slice key("key");
  Slice branch("branch");
  Slice map_key1("key");
  Slice map_val("val");
  Slice map_key2("key");

  VMap vmap;
  auto put1_rsp = db.Put(key, vmap, branch);
  CHECK_EQ(put1_rsp.stat, ErrorCode::kOK);

// Read out that empty set
  auto  get1_rsp = db.Get(key, branch);
  CHECK_EQ(get1_rsp.stat, ErrorCode::kOK);
  VMap vmap1 = get1_rsp.value.Map();
  CHECK_EQ(vmap1.numElements(), 0UL);

  vmap1.Set({map_key1, map_val}, {map_key2, map_val});
  auto put2_rsp = db.Put(key, vmap1, branch);
  CHECK_EQ(put2_rsp.stat, ErrorCode::kOK);


// Read out the previous key
  auto  get2_rsp = db.Get(key, branch);
  CHECK_EQ(get2_rsp.stat, ErrorCode::kOK);
  VMap vmap2 = get2_rsp.value.Map();
  CHECK_EQ(vmap2.numElements(), 1UL);

  // VMap vmap3({map_key1, map_val}, {map_key2, map_val});
  // auto put1_rsp = db.Put(key, vmap3, branch);
  // CHECK_EQ(put1_rsp.stat, ErrorCode::kOK);
}
