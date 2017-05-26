// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_COMPARATOR_H_
#define USTORE_NODE_NODE_COMPARATOR_H_

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "chunk/chunk_loader.h"
#include "hash/hash.h"
#include "node/cursor.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {
// the following two traits dictate how to compute key
//   from element, metaentry and seqnode for comparing and traversing
struct IndexTrait {
  // return the key of element pointed by the given cursor and that
  //   element index
  static OrderedKey Key(const NodeCursor& cursor, uint64_t idx) {
    return OrderedKey(idx);
  }

  // return the max key of all elements rooted under the given metaentry
  //   idx is the index of first elemnet rooted under me
  static OrderedKey MaxKey(const MetaEntry* me, uint64_t idx) {
    return OrderedKey(idx + me->numElements());
  }

  // return the max key of all elements rooted under the given seqnode
  //   idx is the index of first elemnet rooted under mn
  static OrderedKey MaxKey(const SeqNode* mn, uint64_t idx) {
    return OrderedKey(idx + mn->numElements());
  }

  // The smallest key among all
  static OrderedKey MinKey() {
    return OrderedKey(0);
  }
};

struct OrderedKeyTrait {
  static OrderedKey Key(const NodeCursor& cursor, uint64_t idx) {
    return cursor.currentKey();
  }

  static OrderedKey MaxKey(const MetaEntry* me, uint64_t idx) {
    return me->orderedKey();
  }

  static OrderedKey MaxKey(const SeqNode* mn, uint64_t idx) {
    return mn->key(mn->numEntries() - 1);
  }

  static OrderedKey MinKey() {
    static constexpr byte_t kEMPTY[] = "\0";
    return OrderedKey(false, kEMPTY, 1);
  }
};

template <class KeyTrait>
class NodeComparator : Noncopyable {
 public:
// loader is used for both lhs and rhs
  NodeComparator(const Hash& rhs,
                 std::shared_ptr<ChunkLoader> loader) noexcept;

  virtual ~NodeComparator() = default;

// return the index range for elements both occur in lhs and rhs
  virtual std::vector<IndexRange> Intersect(const Hash& lhs) const;

// return the index range for elements only occur in lhs NOT in rhs
  virtual std::vector<IndexRange> Diff(const Hash& lhs) const;

 private:
    // Function to scan element by element to perform diff or intersection
  using IterateProcedure =
      std::function<std::vector<IndexRange>(const Hash& lhs_root,
                                            uint64_t lhs_start_idx,
                                            const Hash& rhs_root,
                                            uint64_t rhs_start_idx,
                                            ChunkLoader*)>;

  // Function to perform diff or intersection when encounting the same hash
  using IdenticalProcedure =
      std::function<std::vector<IndexRange>(const SeqNode* lhs,
                                            uint64_t lhs_start_idx)>;

  // Return the hash of deepest seq node in rhs which is guranteed to contain
  //   all the lhs elements with key between lhs_lower and lhs_upper
  //   as long as such elements occur in rhs

  // The index of its first element is also returned from last parameter

  // This function starts to search for this deepest seq node from rhs_seq_node
  std::shared_ptr<const SeqNode> SmallestOverlap(
      const OrderedKey& lhs_lower, const OrderedKey& lhs_upper,
      std::shared_ptr<const SeqNode> rhs_seq_node,
      uint64_t rhs_start_idx, uint64_t* return_start_idx) const;

  // Compare the lhs tree with rhs in preorder format
  std::vector<IndexRange> Compare(
      const SeqNode* lhs, uint64_t lhs_start_idx, const OrderedKey& lhs_min_key,
      std::shared_ptr<const SeqNode> rhs_root_node, uint64_t rhs_start_idx,
      IdenticalProcedure identical_procedure,
      IterateProcedure iterate_procedure) const;

  // Iterate all elements in both lhs and rhs to detect co-occuring elements
  static std::vector<IndexRange> IterateIntersect(const Hash& lhs,
                                                  uint64_t lhs_start_idx,
                                                  const Hash& rhs,
                                                  uint64_t rhs_start_idx,
                                                  ChunkLoader* loader);

  // Iterate all elements in both lhs and rhs to detect lhs-only element
  static std::vector<IndexRange> IterateDiff(const Hash& lhs,
                                             uint64_t lhs_start_idx,
                                             const Hash& rhs,
                                             uint64_t rhs_start_idx,
                                             ChunkLoader* loader);

  // loader for both lhs and rhs
  mutable std::shared_ptr<ChunkLoader> loader_;

  std::shared_ptr<const SeqNode> rhs_root_;
};

template <class KeyTrait>
NodeComparator<KeyTrait>::NodeComparator(const Hash& rhs,
                               std::shared_ptr<ChunkLoader>
                                   loader) noexcept :
    loader_(loader) {
  const Chunk* chunk = loader_->Load(rhs);
  rhs_root_ = SeqNode::CreateFromChunk(chunk);
}

template <class KeyTrait>
std::vector<IndexRange> NodeComparator<KeyTrait>
    ::Intersect(const Hash& lhs) const {
  std::unique_ptr<const SeqNode> lhs_root =
        SeqNode::CreateFromChunk(loader_->Load(lhs));

  uint64_t lhs_start_idx = 0;
  uint64_t rhs_start_idx = 0;

// If seq node of lhs and rhs with same hash,
//   there rooted elements are also identical
//   return the index range of lhs rooted elements
  IdenticalProcedure identical_intersect =
      [](const SeqNode* lhs, uint64_t lhs_start_idx) {
          std::vector<IndexRange> result;
          result.push_back({lhs_start_idx, lhs->numElements()});
          return result;};

  IterateProcedure iterate_intersect =
      NodeComparator<KeyTrait>::IterateIntersect;

  return IndexRange::Compact(Compare(lhs_root.get(),
                             lhs_start_idx,
                             KeyTrait::MinKey(),
                             rhs_root_,
                             rhs_start_idx,
                             identical_intersect,
                             iterate_intersect));
}

template <class KeyTrait>
std::vector<IndexRange> NodeComparator<KeyTrait>::Diff(const Hash& lhs) const {
  std::unique_ptr<const SeqNode> lhs_root =
        SeqNode::CreateFromChunk(loader_->Load(lhs));

  uint64_t lhs_start_idx = 0;
  uint64_t rhs_start_idx = 0;

  IdenticalProcedure identical_diff =
      [](const SeqNode* lhs, uint64_t lhs_start_idx) {
        // return empty vector
          std::vector<IndexRange> result;
          return result;};

// If seq node of lhs and rhs with same hash,
//   there rooted elements are also identical
//   return empty index ranges for diff
  IterateProcedure iterate_diff = NodeComparator<KeyTrait>::IterateDiff;

  return IndexRange::Compact(Compare(lhs_root.get(),
                            lhs_start_idx,
                            KeyTrait::MinKey(),
                            rhs_root_,
                            rhs_start_idx,
                            identical_diff,
                            iterate_diff));
}

template <class KeyTrait>
std::vector<IndexRange> NodeComparator<KeyTrait>::Compare(
    const SeqNode* lhs, uint64_t lhs_start_idx, const OrderedKey& lhs_min_key,
    const std::shared_ptr<const SeqNode> rhs_node, uint64_t rhs_start_idx,
    IdenticalProcedure identical_procedure,
    IterateProcedure iterate_procedure) const {
  // rhs_root_node is guaranteed to contain all the elements rooted in lhs
  std::vector<IndexRange> results;

  // DLOG(INFO) << "Start Comparing LHS and RHS: \n"
  //            << "LHS Hash: " << lhs->hash().ToBase32()
  //            << " Start Idx: " << lhs_start_idx << "\n"
  //            << "RHS Hash: " << rhs_node->hash().ToBase32()
  //            << " Start Idx: " << rhs_start_idx << "\n";

  // uint64_t lower = lhs_start_idx;
  // uint64_t upper = lhs_start_idx + lhs->numElements();

  // the index of the first element rooted at rhs_closest_node
  uint64_t deepest_start_idx = 0;
  // share_ptr to seqnode
  auto rhs_deepest_node = SmallestOverlap(lhs_min_key,
                                          KeyTrait::MaxKey(lhs, lhs_start_idx),
                                          rhs_node,
                                          rhs_start_idx,
                                          &deepest_start_idx);

  // DLOG(INFO) << "Closest Overlap: \n"
  //            << "Hash: " << rhs_closest_node->hash()
  //            << " Start Idx: " << closest_start_idx;
  if (lhs->hash() == rhs_deepest_node->hash()) {
    return identical_procedure(lhs, lhs_start_idx);
  }

  if (lhs->isLeaf() || rhs_deepest_node->isLeaf()) {
    return iterate_procedure(lhs->hash(),
                             lhs_start_idx,
                             rhs_deepest_node->hash(),
                             deepest_start_idx,
                             loader_.get());
  }  // end if

  // Preorder Traversal
  uint64_t lhs_child_start_idx = lhs_start_idx;
  OrderedKey lhs_child_min_key = lhs_min_key;

  const MetaNode* lhs_meta = dynamic_cast<const MetaNode*>(lhs);
  for (size_t i = 0; i < lhs->numEntries(); i++) {
    MetaEntry lhs_me(lhs_meta->data(i));

    std::unique_ptr<const SeqNode> lhs_child_node =
        SeqNode::CreateFromChunk(loader_->Load(lhs_me.targetHash()));

    std::vector<IndexRange> child_results =
        Compare(lhs_child_node.get(),
                lhs_child_start_idx,
                lhs_child_min_key,
                rhs_deepest_node,
                deepest_start_idx,
                identical_procedure,
                iterate_procedure);

// Concat child_results at the end of final results by moving
    results.insert(results.end(), child_results.begin(), child_results.end());

// Prepare for next iteration
    lhs_child_min_key = KeyTrait::MaxKey(lhs_child_node.get(),
                                          lhs_child_start_idx);

    lhs_child_start_idx += lhs_me.numElements();
  }
  return results;
}

template <class KeyTrait>
std::shared_ptr<const SeqNode> NodeComparator<KeyTrait>::SmallestOverlap(
                                            const OrderedKey& lhs_lower,
                                            const OrderedKey& lhs_upper,
                                            std::shared_ptr<const SeqNode>
                                                rhs_node,
                                            uint64_t rhs_start_idx,
                                            uint64_t* closest_start_idx) const {
  // Precondition, seq_node shall at least contain the range
  // CHECK_LE(rhs_start_idx, lhs_lower);
  if (!rhs_node->isLeaf()) {
    const MetaNode* meta_node = dynamic_cast<const MetaNode*>(rhs_node.get());

    size_t numEntries = meta_node->numEntries();

    // accumalated start idx for each metaentry
    uint64_t rhs_me_start_idx = rhs_start_idx;

    OrderedKey rhs_me_lower;
    OrderedKey rhs_me_upper;

    for (size_t i = 0; i < numEntries; ++i) {
      MetaEntry rhs_me(meta_node->data(i));

      // no further meta_entry will contain this range
      if (i > 0 && rhs_me_lower > lhs_lower) break;

      rhs_me_upper = KeyTrait::MaxKey(&rhs_me, rhs_me_start_idx);

      if (i == numEntries - 1 || rhs_me_upper >= lhs_upper) {
        // Find a deeper overlap
        const Chunk* rhs_child_chunk = loader_->Load(rhs_me.targetHash());
        std::shared_ptr<const SeqNode> rhs_child =
                         SeqNode::CreateFromChunk(rhs_child_chunk);

        return SmallestOverlap(lhs_lower,
                               lhs_upper,
                               rhs_child,
                               rhs_me_start_idx,
                               closest_start_idx);
      }
      rhs_me_start_idx += rhs_me.numElements();
      rhs_me_lower = rhs_me_upper;
    }  // end for
  }  // end if

  *closest_start_idx = rhs_start_idx;
  return rhs_node;
}

template <class KeyTrait>
std::vector<IndexRange> NodeComparator<KeyTrait>::IterateIntersect(
    const Hash& lhs, uint64_t lhs_start_idx,
    const Hash& rhs, uint64_t rhs_start_idx,
    ChunkLoader* loader) {

  std::vector<IndexRange> results;

  NodeCursor lhs_cursor(lhs, 0, loader);
  NodeCursor rhs_cursor(rhs, 0, loader);

  uint64_t lhs_idx = lhs_start_idx;
  uint64_t rhs_idx = rhs_start_idx;

  // if curr_cr.num_subsequent = 0,
  //   this curr_cr is invalid.
  IndexRange curr_cr{0, 0};

  while (!lhs_cursor.isEnd() && !rhs_cursor.isEnd()) {
    OrderedKey lhs_key = KeyTrait::Key(lhs_cursor, lhs_idx);
    OrderedKey rhs_key = KeyTrait::Key(rhs_cursor, rhs_idx);

    if (lhs_key > rhs_key) {
      ++rhs_idx;
      rhs_cursor.Advance(true);
    } else if (lhs_key == rhs_key) {
      size_t lhs_len = lhs_cursor.numCurrentBytes();
      size_t rhs_len = rhs_cursor.numCurrentBytes();

      if (lhs_len == rhs_len &&
          std::memcmp(lhs_cursor.current(),
                      rhs_cursor.current(),
                      lhs_len) == 0) {
      // Identical elements
        if (curr_cr.num_subsequent == 0) {
          curr_cr.start_idx = lhs_idx;
          curr_cr.num_subsequent = 1;
        } else {
          ++curr_cr.num_subsequent;
        }
      } else {
        if (curr_cr.num_subsequent != 0) {
          results.push_back(curr_cr);
          curr_cr.num_subsequent = 0;
        }  // end if
      }  // data comparison

      ++lhs_idx;
      ++rhs_idx;
      lhs_cursor.Advance(true);
      rhs_cursor.Advance(true);
    } else {
      // lhs_idx < rhs_idx
      //   element pointed by lhs cursor
      //   does not appear ih rhs
      if (curr_cr.num_subsequent != 0) {
        results.push_back(curr_cr);
        curr_cr.num_subsequent = 0;
      }  // end if
      ++lhs_idx;
      lhs_cursor.Advance(true);
    }
  }  // end while

  if (curr_cr.num_subsequent != 0) {
    results.push_back(curr_cr);
  }  // end if

  return results;
}

template <class KeyTrait>
std::vector<IndexRange> NodeComparator<KeyTrait>::IterateDiff(
    const Hash& lhs, uint64_t lhs_start_idx,
    const Hash& rhs, uint64_t rhs_start_idx,
    ChunkLoader* loader) {

  // DLOG(INFO) << "Iterate Diff: \n"
  //            << "LHS: " << lhs << " Start_Idx: " << lhs_start_idx << "\n"
  //            << "RHS: " << rhs << " Start_Idx: " << rhs_start_idx;

  std::vector<IndexRange> results;

  NodeCursor lhs_cursor(lhs, 0, loader);
  NodeCursor rhs_cursor(rhs, 0, loader);

  uint64_t lhs_idx = lhs_start_idx;
  uint64_t rhs_idx = rhs_start_idx;

  // whether num_subsequent field = 0 in curr_cr to
  //   mark whether this curr_cr is valid or not.
  IndexRange curr_cr{0, 0};

  while (!lhs_cursor.isEnd() && !rhs_cursor.isEnd()) {
    const OrderedKey lhs_key = KeyTrait::Key(lhs_cursor, lhs_idx);
    const OrderedKey rhs_key = KeyTrait::Key(rhs_cursor, rhs_idx);

    if (lhs_key > rhs_key) {
      ++rhs_idx;
      rhs_cursor.Advance(true);
    } else if (lhs_key == rhs_key) {
      size_t lhs_len = lhs_cursor.numCurrentBytes();
      size_t rhs_len = rhs_cursor.numCurrentBytes();

      if (lhs_len == rhs_len &&
          std::memcmp(lhs_cursor.current(),
                      rhs_cursor.current(),
                      lhs_len) == 0) {
      // Identical elements,
      //   Stop updating curr_cr if valid
        if (curr_cr.num_subsequent != 0) {
          results.push_back(curr_cr);
          curr_cr.num_subsequent = 0;  // mark curr_cr is invalid
        }  // end if
      } else {
        if (curr_cr.num_subsequent == 0) {
          // DLOG(INFO) << "Diff Start Idx: "
          //           << lhs_idx;
          curr_cr.start_idx = lhs_idx;
          curr_cr.num_subsequent = 1;
        } else {
          ++curr_cr.num_subsequent;
          // DLOG(INFO) << "  Incrementing for idx " << lhs_idx
          //            << " to " << curr_cr.num_subsequent;
        }
      }  // data comparison

      ++lhs_idx;
      ++rhs_idx;
      lhs_cursor.Advance(true);
      rhs_cursor.Advance(true);
    } else {
      // lhs_idx < rhs_idx
      //   rhs does not contain the element pointed by lhs
      if (curr_cr.num_subsequent == 0) {
        curr_cr.start_idx = lhs_idx;
        curr_cr.num_subsequent = 1;
      } else {
        ++curr_cr.num_subsequent;
      }
      ++lhs_idx;
      lhs_cursor.Advance(true);
    }
  }  // end while

  // if lhs not to the end,
  //   the rest of elements are not contained in rhs
  // Include all of them in index range

  if (!lhs_cursor.isEnd()) {
    if (curr_cr.num_subsequent == 0) {
      curr_cr.start_idx = lhs_idx;
      curr_cr.num_subsequent = 0;
    }  // end if

    do {
      ++curr_cr.num_subsequent;
      // DLOG(INFO) << "  Incrementing for idx " << lhs_idx
      //            << " to " << curr_cr.num_subsequent;
      ++lhs_idx;
    } while (lhs_cursor.Advance(true));  // end while
  }  // end if

  if (curr_cr.num_subsequent != 0) {
    results.push_back(curr_cr);
  }  // end if

  return results;
}


using IndexComparator = NodeComparator<IndexTrait>;

using KeyComparator = NodeComparator<OrderedKeyTrait>;

}  // namespace ustore

#endif  // USTORE_NODE_NODE_COMPARATOR_H_
