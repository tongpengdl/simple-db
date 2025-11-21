#include "simpledb/record.h"

#include <cstring>
#include <limits>

#include "simpledb/page.h"
#include "simpledb/status.h"

namespace simpledb {

SlottedPage::SlottedPage(Page& page) : page_(page) {
  auto& hdr = header();
  if (hdr.slot_count == 0 && hdr.free_start == 0) {
    hdr.free_start = static_cast<std::uint16_t>(sizeof(Header));
    hdr.slot_count = 0;
  }
}

Result<std::uint16_t> SlottedPage::Insert(std::span<const std::byte> record) {
  if (record.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    return Status::InvalidArgument("record too large for page");
  }

  const auto& hdr = header();
  const std::size_t used_for_slots =
      (static_cast<std::size_t>(hdr.slot_count) + 1) * sizeof(Slot);
  const std::size_t space_limit =
      kPageSize > used_for_slots ? kPageSize - used_for_slots : 0;

  if (hdr.free_start + record.size() > space_limit) {
    return Status::InvalidArgument("not enough free space on page");
  }

  auto& mutable_hdr = header();
  const std::uint16_t slot_id = mutable_hdr.slot_count;
  const auto offset = mutable_hdr.free_start;

  std::memcpy(page_.data.data() + offset, record.data(), record.size());
  mutable_hdr.free_start =
      static_cast<std::uint16_t>(mutable_hdr.free_start + record.size());
  mutable_hdr.slot_count =
      static_cast<std::uint16_t>(mutable_hdr.slot_count + 1);

  auto* slot = slot_ptr(slot_id);
  slot->offset = offset;
  slot->size = static_cast<std::uint16_t>(record.size());

  return slot_id;
}

Result<RecordView> SlottedPage::Get(std::uint16_t slot_id) const {
  if (slot_id >= header().slot_count) {
    return Status::NotFound("slot id out of range");
  }

  const Slot* slot = slot_ptr(slot_id);
  if (slot->offset + slot->size > kPageSize) {
    return Status::Internal("slot metadata points outside page");
  }

  const auto* start = page_.data.data() + slot->offset;
  std::span<const std::byte> data(start, slot->size);
  return RecordView{data};
}

std::uint16_t SlottedPage::slot_count() const { return header().slot_count; }

std::size_t SlottedPage::free_space() const {
  const auto& hdr = header();
  const std::size_t used_for_slots =
      (static_cast<std::size_t>(hdr.slot_count) + 1) * sizeof(Slot);
  const std::size_t space_limit =
      kPageSize > used_for_slots ? kPageSize - used_for_slots : 0;
  if (hdr.free_start >= space_limit) {
    return 0;
  }
  return space_limit - hdr.free_start;
}

SlottedPage::Header& SlottedPage::header() {
  return *reinterpret_cast<Header*>(page_.data.data());
}

const SlottedPage::Header& SlottedPage::header() const {
  return *reinterpret_cast<const Header*>(page_.data.data());
}

SlottedPage::Slot* SlottedPage::slot_ptr(std::uint16_t index) {
  return reinterpret_cast<Slot*>(page_.data.data() + kPageSize) - (index + 1);
}

const SlottedPage::Slot* SlottedPage::slot_ptr(std::uint16_t index) const {
  return reinterpret_cast<const Slot*>(page_.data.data() + kPageSize) -
         (index + 1);
}

}  // namespace simpledb
