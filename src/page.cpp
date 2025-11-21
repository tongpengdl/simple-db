#include "simpledb/page.h"

#include <algorithm>

namespace simpledb {

void ClearPage(Page& page) {
  std::fill(page.data.begin(), page.data.end(), std::byte{0});
}

}  // namespace simpledb
