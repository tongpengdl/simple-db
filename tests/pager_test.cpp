#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

#include "simpledb/pager.h"
#include "simpledb/record.h"

int main() {
  namespace fs = std::filesystem;
  using namespace simpledb;

  const fs::path path = fs::temp_directory_path() / "simpledb_pager_test.db";
  fs::remove(path);

  Pager pager;
  Status status = pager.Open(path);
  assert(status.ok());
  assert(pager.page_count() == 0);

  auto page_result = pager.Allocate();
  assert(page_result.ok());

  Page page = std::move(page_result.value());
  SlottedPage slotted(page);

  const std::string payload = "pager roundtrip payload";
  const std::span<const std::byte> bytes{
      reinterpret_cast<const std::byte*>(payload.data()), payload.size()};

  auto slot_id = slotted.Insert(bytes);
  assert(slot_id.ok());
  assert(slotted.slot_count() == 1);

  status = pager.Flush(page);
  assert(status.ok());
  assert(pager.page_count() == 1);

  auto loaded = pager.Load(page.id);
  assert(loaded.ok());

  SlottedPage reloaded(loaded.value());
  auto record_view = reloaded.Get(slot_id.value());
  assert(record_view.ok());

  const auto& span = record_view.value().data;
  const std::string retrieved(reinterpret_cast<const char*>(span.data()),
                              span.size());
  assert(retrieved == payload);

  std::cout << "pager_test: success\n";

  fs::remove(path);
  return 0;
}
