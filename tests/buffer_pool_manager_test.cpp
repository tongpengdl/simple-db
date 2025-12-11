#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "simpledb/buffer_pool_manager.h"
#include "simpledb/disk_manager.h"
#include "simpledb/record.h"

int main() {
  namespace fs = std::filesystem;
  using namespace simpledb;

  const fs::path path =
      fs::temp_directory_path() / "simpledb_buffer_pool_manager_test.db";
  fs::remove(path);

  auto disk_manager = std::make_unique<DiskManager>();
  auto status = disk_manager->Open(path);
  assert(status.ok());

  auto buffer_pool_manager =
      std::make_unique<BufferPoolManager>(2, disk_manager.get());

  // Test NewPage
  auto page_res = buffer_pool_manager->NewPage();
  assert(page_res.ok());
  Page* page0 = page_res.value();
  assert(page0->id == 0);

  // Write something to page 0
  SlottedPage slotted0(*page0);
  const std::string payload0 = "payload for page 0";
  auto span0 = std::as_bytes(std::span(payload0));
  auto slot_id0 = slotted0.Insert(span0);
  assert(slot_id0.ok());

  // Unpin page 0 and mark it dirty
  status = buffer_pool_manager->UnpinPage(page0->id, true);
  assert(status.ok());

  // Test FetchPage
  auto fetch_res = buffer_pool_manager->FetchPage(0);
  assert(fetch_res.ok());
  Page* fetched_page0 = fetch_res.value();
  assert(fetched_page0->id == 0);

  SlottedPage reloaded0(*fetched_page0);
  auto record_view0 = reloaded0.Get(slot_id0.value());
  assert(record_view0.ok());
  const auto& rv0 = record_view0.value();
  std::string retrieved0(reinterpret_cast<const char*>(rv0.data.data()),
                         rv0.data.size());
  assert(retrieved0 == payload0);

  // Unpin page 0 again
  status = buffer_pool_manager->UnpinPage(fetched_page0->id, false);
  assert(status.ok());

  // Test eviction
  page_res = buffer_pool_manager->NewPage(); // Creates page 1
  assert(page_res.ok());
  Page* page1 = page_res.value();
  assert(page1->id == 1);
  status = buffer_pool_manager->UnpinPage(page1->id, false);
  assert(status.ok());

  page_res = buffer_pool_manager->NewPage(); // Evicts page 0, creates page 2
  assert(page_res.ok());
  Page* page2 = page_res.value();
  assert(page2->id == 2);
  status = buffer_pool_manager->UnpinPage(page2->id, false);
  assert(status.ok());

  // Page 0 should have been flushed to disk, so we can fetch it again.
  fetch_res = buffer_pool_manager->FetchPage(0);
  assert(fetch_res.ok());
  fetched_page0 = fetch_res.value();
  assert(fetched_page0->id == 0);

  SlottedPage reloaded_after_eviction(*fetched_page0);
  record_view0 = reloaded_after_eviction.Get(slot_id0.value());
  assert(record_view0.ok());
  const auto& rv1 = record_view0.value();
  std::string retrieved1(reinterpret_cast<const char*>(rv1.data.data()),
                         rv1.data.size());
  assert(retrieved1 == payload0);
  status = buffer_pool_manager->UnpinPage(fetched_page0->id, false);
  assert(status.ok());

  std::cout << "buffer_pool_manager_test: success\n";

  fs::remove(path);
  return 0;
}
