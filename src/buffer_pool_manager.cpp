#include "simpledb/buffer_pool_manager.h"

#include <utility>

namespace simpledb {

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  frames_.resize(pool_size_);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() { FlushAllPages(); }

Result<Page*> BufferPoolManager::FetchPage(PageId page_id) {
  std::scoped_lock lock(latch_);

  if (page_table_.count(page_id) > 0) {
    const auto frame_id = page_table_[page_id];
    frames_[frame_id].pin_count++;
    replacer_.remove(frame_id);
    return &frames_[frame_id].page;
  }

  auto victim_res = GetVictim();
  if (!victim_res.ok()) {
    return victim_res.status();
  }
  const auto frame_id = victim_res.value();

  auto& victim_frame = frames_[frame_id];
  if (victim_frame.page.id != kInvalidPageId) {
    if (victim_frame.is_dirty) {
      auto status = disk_manager_->WritePage(
          victim_frame.page.id,
          reinterpret_cast<char*>(victim_frame.page.data.data()));
      if (!status.ok()) {
        replacer_.emplace_front(frame_id);
        return status;
      }
    }
    page_table_.erase(victim_frame.page.id);
  }

  auto status = disk_manager_->ReadPage(
      page_id, reinterpret_cast<char*>(victim_frame.page.data.data()));
  if (!status.ok()) {
    victim_frame.page.id = kInvalidPageId;
    victim_frame.pin_count = 0;
    victim_frame.is_dirty = false;
    ClearPage(victim_frame.page);
    free_list_.emplace_front(frame_id);
    return status;
  }

  victim_frame.page.id = page_id;
  victim_frame.pin_count = 1;
  victim_frame.is_dirty = false;

  page_table_[page_id] = frame_id;

  return &victim_frame.page;
}

Status BufferPoolManager::UnpinPage(PageId page_id, bool is_dirty) {
  std::scoped_lock lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return Status::Internal("page not in buffer pool");
  }

  const auto frame_id = page_table_[page_id];
  auto& frame = frames_[frame_id];

  if (frame.pin_count <= 0) {
    return Status::Internal("unpinning a page with pin count 0");
  }

  frame.pin_count--;
  if (is_dirty) {
    frame.is_dirty = true;
  }

  if (frame.pin_count == 0) {
    replacer_.emplace_front(frame_id);
  }

  return Status::OK();
}

Status BufferPoolManager::FlushPage(PageId page_id) {
  std::scoped_lock lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return Status::Internal("page not in buffer pool");
  }

  const auto frame_id = page_table_[page_id];
  auto& frame = frames_[frame_id];

  auto status =
      disk_manager_->WritePage(frame.page.id, reinterpret_cast<char*>(frame.page.data.data()));
  if (status.ok()) {
    frame.is_dirty = false;
  }

  return status;
}

Result<Page*> BufferPoolManager::NewPage() {
  std::scoped_lock lock(latch_);

  auto victim_res = GetVictim();
  if (!victim_res.ok()) {
    return victim_res.status();
  }
  const auto frame_id = victim_res.value();
  auto& new_frame = frames_[frame_id];
  if (new_frame.page.id != kInvalidPageId) {
    if (new_frame.is_dirty) {
      auto status = disk_manager_->WritePage(
          new_frame.page.id,
          reinterpret_cast<char*>(new_frame.page.data.data()));
      if (!status.ok()) {
        replacer_.emplace_front(frame_id);
        return status;
      }
    }
    page_table_.erase(new_frame.page.id);
  }

  auto page_id_res = disk_manager_->AllocatePage();
  if (!page_id_res.ok()) {
    // If we can't allocate a new page, put the frame back on the free list
    new_frame.page.id = kInvalidPageId;
    new_frame.pin_count = 0;
    new_frame.is_dirty = false;
    ClearPage(new_frame.page);
    free_list_.emplace_front(frame_id);
    return page_id_res.status();
  }
  const auto page_id = page_id_res.value();
  
  new_frame.page.id = page_id;
  new_frame.pin_count = 1;
  new_frame.is_dirty = false;
  ClearPage(new_frame.page);
  
  page_table_[page_id] = frame_id;

  return &new_frame.page;
}

Status BufferPoolManager::DeletePage(PageId page_id) {
  static_cast<void>(page_id);
  return Status::Unimplemented("DeletePage not implemented");
}

Status BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(latch_);
  for (auto const& [page_id, frame_id] : page_table_) {
    auto& frame = frames_[frame_id];
    if (frame.is_dirty) {
      auto status = disk_manager_->WritePage(
          frame.page.id, reinterpret_cast<char*>(frame.page.data.data()));
      if (status.ok()) {
        frame.is_dirty = false;
      } else {
        return status;
      }
    }
  }
  return Status::OK();
}

Result<BufferPoolManager::frame_id_t> BufferPoolManager::GetVictim() {
  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }

  if (replacer_.empty()) {
    return Status::Internal("out of memory");
  }

  // The replacer stores pages in MRU order, so the LRU is at the back
  auto frame_id = replacer_.back();
  replacer_.pop_back();

  return frame_id;
}

}  // namespace simpledb
