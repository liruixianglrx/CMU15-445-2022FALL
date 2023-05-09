//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType value{array_[index].second};
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  int left;
  int right;
  left = 0;
  right = GetSize() - 1;

  while (left <= right) {
    int mid = (right - left) / 2 + left;
    int compare_result1 = comparator(key, array_[mid].first);
    int compare_result2;

    if (mid + 1 == GetSize()) {
      compare_result2 = -1;
    } else {
      compare_result2 = comparator(key, array_[mid + 1].first);
    }

    if (compare_result1 == 0 || (compare_result1 == 1 && compare_result2 == -1)) {
      return mid;
    }

    if (compare_result1 == -1) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  if (right == -1) {
    return -1;
  }

  return GetSize();
  // auto target = std::lower_bound(array_, array_ + GetSize(), key,
  //                                [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  // return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *target) {
  int split_size = GetMinSize();
  std::copy(array_ + split_size, array_ + GetSize(), target->array_);
  target->SetSize(GetSize() - split_size);
  SetSize(split_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int idx = IndexOfKey(key, comparator);

  if (idx != -1 && idx != GetSize() && comparator(array_[idx].first, key) == 0) {
    return false;
  }

  std::move_backward(array_ + idx + 1, array_ + GetSize(), array_ + GetSize() + 1);
  array_[idx + 1] = std::make_pair(key, value);

  IncreaseSize(1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
