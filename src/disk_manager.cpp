#include "simpledb/disk_manager.h"

#include <filesystem>
#include <fstream>
#include <utility>

#include "simpledb/page.h"
#include "simpledb/status.h"

namespace simpledb {

DiskManager::~DiskManager() {
  if (file_.is_open()) {
    file_.close();
  }
}

Status DiskManager::Open(const std::filesystem::path& path) {
  if (file_.is_open()) {
    file_.close();
  }

  path_ = path;

  if (!std::filesystem::exists(path_)) {
    std::ofstream create(path_, std::ios::binary | std::ios::trunc);
    if (!create) {
      return Status::IoError("failed to create database file");
    }
  }

  file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_) {
    return Status::IoError("failed to open database file");
  }

  file_.seekg(0, std::ios::end);
  const auto size = file_.tellg();
  if (size < 0) {
    return Status::IoError("failed to determine database size");
  }

  if (size % static_cast<std::streamoff>(kPageSize) != 0) {
    return Status::Internal("database file size is not page aligned");
  }

  page_count_ = static_cast<std::size_t>(size) / kPageSize;
  return Status::OK();
}

Result<PageId> DiskManager::AllocatePage() {
  const Status open_status = EnsureOpen();
  if (!open_status.ok()) {
    return open_status;
  }

  Page page;
  page.id = static_cast<PageId>(page_count_);
  ClearPage(page);

  file_.seekp(0, std::ios::end);
  file_.write(reinterpret_cast<const char*>(page.data.data()),
              static_cast<std::streamsize>(page.data.size()));

  if (!file_) {
    return Status::IoError("failed to write new page");
  }

  file_.flush();
  ++page_count_;
  return page.id;
}

Status DiskManager::ReadPage(PageId id, char* data) const {
  const Status open_status = EnsureOpen();
  if (!open_status.ok()) {
    return open_status;
  }

  if (id >= page_count_) {
    return Status::NotFound("page id out of range");
  }

  file_.seekg(static_cast<std::streamoff>(id) * kPageSize, std::ios::beg);
  file_.read(data, kPageSize);

  if (file_.gcount() != kPageSize ||
      !file_) {
    file_.clear();
    return Status::IoError("failed to read page");
  }

  return Status::OK();
}

Status DiskManager::WritePage(PageId id, const char* data) {
  const Status open_status = EnsureOpen();
  if (!open_status.ok()) {
    return open_status;
  }

  if (id >= page_count_) {
    return Status::NotFound("cannot write unknown page id");
  }

  file_.seekp(static_cast<std::streamoff>(id) * kPageSize, std::ios::beg);
  file_.write(data, kPageSize);

  if (!file_) {
    return Status::IoError("failed to write page");
  }

  file_.flush();
  return Status::OK();
}

Status DiskManager::EnsureOpen() const {
  if (file_.is_open()) {
    return Status::OK();
  }

  if (path_.empty()) {
    return Status::InvalidArgument("pager not initialized");
  }

  file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_) {
    return Status::IoError("unable to reopen database file");
  }

  return Status::OK();
}

}  // namespace simpledb
