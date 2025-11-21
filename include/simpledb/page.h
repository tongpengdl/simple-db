#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace simpledb {

using PageId = std::uint64_t;

inline constexpr std::size_t kPageSize = 4096;

struct alignas(std::max_align_t) Page {
  PageId id{0};
  std::array<std::byte, kPageSize> data{};
};

void ClearPage(Page& page);

}  // namespace simpledb
