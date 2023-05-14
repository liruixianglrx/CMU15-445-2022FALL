/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <common/logger.h>
#include <cassert>

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index, Page *page, BufferPoolManager *bpm)
    : index_(index), page_(page), bpm_(bpm) {
  leaf_page_ = reinterpret_cast<LeafPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  page_->RUnlatch();
  bpm_->UnpinPage(leaf_page_->GetPageId(), false);
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return *(leaf_page_->GetItems() + index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // printf("iterator ++ \n");
  if (index_ == leaf_page_->GetSize() - 1 && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    auto *next_page = bpm_->FetchPage(leaf_page_->GetNextPageId());

    next_page->RLatch();
    page_->RUnlatch();
    bpm_->UnpinPage(page_->GetPageId(), 0);

    page_ = next_page;
    leaf_page_ = reinterpret_cast<LeafPage *>(next_page->GetData());
    index_ = 0;
  } else {
    index_++;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
