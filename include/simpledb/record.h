#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "simpledb/page.h"
#include "simpledb/status.h"

namespace simpledb {

struct RecordView {
  std::span<const std::byte> data;
};

class SlottedPage {
 public:
  explicit SlottedPage(Page& page);

  Result<std::uint16_t> Insert(std::span<const std::byte> record);

  Result<RecordView> Get(std::uint16_t slot_id) const;

  std::uint16_t slot_count() const;

  std::size_t free_space() const;

 private:
  struct Header {
    std::uint16_t free_start;
    std::uint16_t slot_count;
  };

  struct Slot {
    std::uint16_t offset;
    std::uint16_t size;
  };

  Header& header();
  const Header& header() const;
  Slot* slot_ptr(std::uint16_t index);
  const Slot* slot_ptr(std::uint16_t index) const;

  Page& page_;
};

}  // namespace simpledb
