#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "simpledb/page.h"
#include "simpledb/pager.h"
#include "simpledb/record.h"
#include "simpledb/status.h"

int main() {
  using namespace simpledb;

  Pager pager;
  const auto db_path = std::filesystem::path("simple.db");

  Status status = pager.Open(db_path);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.message() << "\n";
    return 1;
  }

  Result<Page> page_result = pager.page_count() == 0 ? pager.Allocate()
                                                     : pager.Load(0);
  if (!page_result.ok()) {
    std::cerr << "Failed to get page: " << page_result.status().message()
              << "\n";
    return 1;
  }

  Page page = std::move(page_result.value());
  SlottedPage slotted(page);

  const std::string sample = "hello from simple-db";
  const std::span<const std::byte> bytes{
      reinterpret_cast<const std::byte*>(sample.data()), sample.size()};

  const auto slot_id = slotted.Insert(bytes);
  if (!slot_id.ok()) {
    std::cerr << "Insert failed: " << slot_id.status().message() << "\n";
    return 1;
  }

  status = pager.Flush(page);
  if (!status.ok()) {
    std::cerr << "Flush failed: " << status.message() << "\n";
    return 1;
  }

  auto loaded_page = pager.Load(page.id);
  if (!loaded_page.ok()) {
    std::cerr << "Reload failed: " << loaded_page.status().message() << "\n";
    return 1;
  }

  SlottedPage reloaded(loaded_page.value());
  auto record_view = reloaded.Get(slot_id.value());
  if (!record_view.ok()) {
    std::cerr << "Read failed: " << record_view.status().message() << "\n";
    return 1;
  }

  const auto& viewed = record_view.value().data;
  const std::string roundtrip(reinterpret_cast<const char*>(viewed.data()),
                              viewed.size());

  std::cout << "Page " << page.id << " slots=" << reloaded.slot_count()
            << " free=" << reloaded.free_space() << " bytes\n";
  std::cout << "Round-trip record: " << roundtrip << "\n";
  return 0;
}
