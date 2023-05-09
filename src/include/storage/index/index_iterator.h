//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator(int index, LeafPage *leaf_page, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (itr.leaf_page_->GetPageId() == leaf_page_->GetPageId() && itr.index_ == index_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(itr.leaf_page_->GetPageId() == leaf_page_->GetPageId() && itr.index_ == index_);
  }

 private:
  int index_;
  LeafPage *leaf_page_;
  BufferPoolManager *bpm_;
  // add your own private member variables here
};

}  // namespace bustub
