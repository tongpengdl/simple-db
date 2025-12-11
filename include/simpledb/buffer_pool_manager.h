#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "simpledb/disk_manager.h"
#include "simpledb/page.h"

namespace simpledb {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size, DiskManager* disk_manager);

  ~BufferPoolManager();

  BufferPoolManager(const BufferPoolManager&) = delete;
  BufferPoolManager& operator=(const BufferPoolManager&) = delete;

  Result<Page*> FetchPage(PageId page_id);

  Status UnpinPage(PageId page_id, bool is_dirty);

  Status FlushPage(PageId page_id);

  Result<Page*> NewPage();

  Status DeletePage(PageId page_id);

  Status FlushAllPages();

 private:
  using frame_id_t = size_t;

  struct Frame {
    Page page;
    bool is_dirty{false};
    int pin_count{0};
  };

  Result<frame_id_t> GetVictim();

  size_t pool_size_;
  std::vector<Frame> frames_;
  DiskManager* disk_manager_;
  std::unordered_map<PageId, frame_id_t> page_table_;
  std::list<frame_id_t> replacer_;
  std::list<frame_id_t> free_list_;
  std::mutex latch_;
};

}  // namespace simpledb
