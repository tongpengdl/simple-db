#ifndef SIMPLEDB_DISK_MANAGER_H
#define SIMPLEDB_DISK_MANAGER_H

#include <filesystem>
#include <fstream>

#include "simpledb/page.h"
#include "simpledb/status.h"

namespace simpledb {

class DiskManager {
 public:
  DiskManager() = default;
  ~DiskManager();

  DiskManager(const DiskManager&) = delete;
  DiskManager& operator=(const DiskManager&) = delete;

  Status Open(const std::filesystem::path& path);

  Result<PageId> AllocatePage();

  Status ReadPage(PageId id, char* data) const;

  Status WritePage(PageId id, const char* data);

  std::size_t page_count() const { return page_count_; }
  bool is_open() const { return file_.is_open(); }
  const std::filesystem::path& path() const { return path_; }

 private:
  Status EnsureOpen() const;

  std::filesystem::path path_;
  mutable std::fstream file_;
  std::size_t page_count_{0};
};

}  // namespace simpledb

#endif  // SIMPLEDB_DISK_MANAGER_H
