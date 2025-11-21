#pragma once

#include <filesystem>
#include <fstream>

#include "simpledb/page.h"
#include "simpledb/status.h"

namespace simpledb {

class Pager {
 public:
  Pager() = default;
  ~Pager();

  Pager(const Pager&) = delete;
  Pager& operator=(const Pager&) = delete;

  Status Open(const std::filesystem::path& path);

  Result<Page> Allocate();

  Result<Page> Load(PageId id) const;

  Status Flush(const Page& page);

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
