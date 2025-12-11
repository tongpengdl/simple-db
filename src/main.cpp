#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "simpledb/buffer_pool_manager.h"
#include "simpledb/disk_manager.h"
#include "simpledb/page.h"
#include "simpledb/record.h"
#include "simpledb/status.h"

int main() {
  using namespace simpledb;

  const auto db_path = std::filesystem::path("simple.db");
  auto disk_manager = std::make_unique<DiskManager>();
  auto status = disk_manager->Open(db_path);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.message() << "\n";
    return 1;
  }

  auto buffer_pool_manager =
      std::make_unique<BufferPoolManager>(10, disk_manager.get());

  Result<Page*> page_result = disk_manager->page_count() == 0
                                  ? buffer_pool_manager->NewPage()
                                  : buffer_pool_manager->FetchPage(0);
  if (!page_result.ok()) {
    std::cerr << "Failed to get page: " << page_result.status().message()
              << "\n";
    return 1;
  }

  Page* page = page_result.value();
  SlottedPage slotted(*page);

  const std::string sample = "hello from simple-db";
  const std::span<const std::byte> bytes{
      reinterpret_cast<const std::byte*>(sample.data()), sample.size()};

  const auto slot_id = slotted.Insert(bytes);
  if (!slot_id.ok()) {
    std::cerr << "Insert failed: " << slot_id.status().message() << "\n";
    buffer_pool_manager->UnpinPage(page->id, false);
    return 1;
  }

  status = buffer_pool_manager->UnpinPage(page->id, true);
  if (!status.ok()) {
    std::cerr << "Unpin failed: " << status.message() << "\n";
    return 1;
  }

  auto loaded_page_res = buffer_pool_manager->FetchPage(page->id);
  if (!loaded_page_res.ok()) {
    std::cerr << "Reload failed: " << loaded_page_res.status().message() << "\n";
    return 1;
  }
  Page* loaded_page = loaded_page_res.value();

  SlottedPage reloaded(*loaded_page);
  auto record_view = reloaded.Get(slot_id.value());
  if (!record_view.ok()) {
    std::cerr << "Read failed: " << record_view.status().message() << "\n";
    buffer_pool_manager->UnpinPage(loaded_page->id, false);
    return 1;
  }

  const auto& viewed = record_view.value().data;
  const std::string roundtrip(reinterpret_cast<const char*>(viewed.data()),
                              viewed.size());

  std::cout << "Page " << page->id << " slots=" << reloaded.slot_count()
            << " free=" << reloaded.free_space() << " bytes\n";
  std::cout << "Round-trip record: " << roundtrip << "\n";

  buffer_pool_manager->UnpinPage(loaded_page->id, false);
  return 0;
}
