#include "columnfile.h"

#include <algorithm>

#include <kj/arena.h>
#include <kj/debug.h>

namespace cantera {

namespace {

// Makes a copy of the given string inside the arena.
std::string_view CopyString(const std::string_view& string, kj::Arena& arena) {
  const auto copy =
      arena.copyString(kj::StringPtr(string.data(), string.size()));
  return std::string_view{copy.begin(), copy.size()};
}

}  // namespace

struct ColumnFileSelect::Impl {
  struct RowCache {
    std::vector<std::pair<uint32_t, optional_string_view>> data;
    uint32_t index;
  };

  Impl(ColumnFileReader input) : input{std::move(input)} {}

  ColumnFileReader input;

  std::unordered_set<uint32_t> selection;

  std::vector<
      std::pair<uint32_t, std::function<bool(const optional_string_view&)>>>
      filters;

  std::unique_ptr<kj::Arena> arena;

  std::vector<RowCache> row_buffer;
  size_t chunk_offset = 0;

  // Columns that appear in `selection`, but not in `filters`.
  std::unordered_set<uint32_t> unfiltered_selection;
};

ColumnFileSelect::ColumnFileSelect(ColumnFileReader input)
    : pimpl_{std::make_unique<Impl>(std::move(input))} {}

ColumnFileSelect::~ColumnFileSelect() = default;

void ColumnFileSelect::AddSelection(uint32_t field) {
  pimpl_->selection.emplace(field);
}

void ColumnFileSelect::AddFilter(
    uint32_t field, std::function<bool(const optional_string_view&)> filter) {
  pimpl_->filters.emplace_back(field, std::move(filter));
}

void ColumnFileSelect::StartScan() {
  if (pimpl_->filters.empty()) {
    pimpl_->input.SetColumnFilter(pimpl_->selection.begin(),
                                  pimpl_->selection.end());
    return;
  }

  pimpl_->row_buffer.clear();

  // Sort filters by column index.
  std::stable_sort(
      pimpl_->filters.begin(), pimpl_->filters.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

  pimpl_->unfiltered_selection = pimpl_->selection;
  for (const auto& filter : pimpl_->filters)
    pimpl_->unfiltered_selection.erase(filter.first);
}

void ColumnFileSelect::ReadChunk() {
  for (;;) {
    pimpl_->row_buffer.clear();
    pimpl_->chunk_offset = 0;

    // Used to hold temporary copies of strings.
    pimpl_->arena = std::make_unique<kj::Arena>();

    size_t filter_idx = 0;

    // Iterate over all filters.
    do {
      auto field = pimpl_->filters[filter_idx].first;

      pimpl_->input.SetColumnFilter({field});
      if (filter_idx == 0) {
        if (pimpl_->input.End()) return;
      } else {
        pimpl_->input.SeekToStartOfSegment();
      }

      auto filter_selected = pimpl_->selection.count(field);

      size_t filter_range_end = filter_idx + 1;

      while (filter_range_end != pimpl_->filters.size() &&
             pimpl_->filters[filter_range_end].first == field)
        ++filter_range_end;

      auto in = pimpl_->row_buffer.begin();
      auto out = pimpl_->row_buffer.begin();

      // Iterate over all values in the current segment for the current column.
      for (uint32_t row_idx = 0; !pimpl_->input.EndOfSegment(); ++row_idx) {
        auto row = pimpl_->input.GetRow();

        if (filter_idx > 0) {
          // Is row already filtered?
          if (row_idx < in->index) continue;

          KJ_ASSERT(in->index == row_idx);
        }

        optional_string_view value = std::nullopt;
        if (row.size() == 1) {
          KJ_ASSERT(row[0].first == field, row[0].first, field);
          value = row[0].second;
        }

        bool match = true;

        for (size_t i = filter_idx; i != filter_range_end; ++i) {
          if (!pimpl_->filters[i].second(value)) {
            match = false;
            break;
          }
        }

        if (match) {
          if (filter_idx == 0) {
            Impl::RowCache row_cache;
            row_cache.index = row_idx;

            if (filter_selected) {
              if (!value)
                row_cache.data.emplace_back(field, std::nullopt);
              else
                row_cache.data.emplace_back(
                    field, CopyString(value.value(), *pimpl_->arena));
            }

            pimpl_->row_buffer.emplace_back(std::move(row_cache));
          } else {
            if (out != in) *out = std::move(*in);

            if (filter_selected) {
              if (!value)
                out->data.emplace_back(field, std::nullopt);
              else
                out->data.emplace_back(
                    field, CopyString(value.value(), *pimpl_->arena));
            }

            ++out;
            ++in;
          }
        }
      }

      if (filter_idx > 0)
        pimpl_->row_buffer.erase(out, pimpl_->row_buffer.end());

      filter_idx = filter_range_end;
    } while (!pimpl_->row_buffer.empty() &&
             filter_idx < pimpl_->filters.size());

    // Now rows passing filter in current chunk, read next one.
    if (pimpl_->row_buffer.empty()) continue;

    if (!pimpl_->unfiltered_selection.empty()) {
      pimpl_->input.SetColumnFilter(pimpl_->unfiltered_selection.begin(),
                                    pimpl_->unfiltered_selection.end());
      pimpl_->input.SeekToStartOfSegment();

      auto sr = pimpl_->row_buffer.begin();

      for (uint32_t row_idx = 0; !pimpl_->input.EndOfSegment(); ++row_idx) {
        if (sr == pimpl_->row_buffer.end()) break;

        auto row = pimpl_->input.GetRow();

        if (row_idx < sr->index) continue;

        KJ_REQUIRE(row_idx == sr->index, row_idx, sr->index);

        for (const auto& d : row) {
          const auto& value = d.second;
          if (!value)
            sr->data.emplace_back(d.first, std::nullopt);
          else
            sr->data.emplace_back(d.first,
                                  CopyString(d.second.value(), *pimpl_->arena));
        }

        ++sr;
      }

      while (!pimpl_->input.EndOfSegment()) pimpl_->input.GetRow();
    }

    break;
  }
}

std::optional<std::reference_wrapper<const std::vector<std::pair<uint32_t, optional_string_view>>>>
ColumnFileSelect::Iterate() {
  if (pimpl_->filters.empty()) {
    if (pimpl_->input.End()) return std::nullopt;

    return pimpl_->input.GetRow();
  }

  if (pimpl_->chunk_offset == pimpl_->row_buffer.size()) {
    ReadChunk();
    if (pimpl_->chunk_offset == pimpl_->row_buffer.size()) return std::nullopt;
  }

  auto& row = pimpl_->row_buffer[pimpl_->chunk_offset++];
  std::sort(
      row.data.begin(), row.data.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

  return row.data;
}

void ColumnFileSelect::Execute(
    std::function<
        void(const std::vector<std::pair<uint32_t, optional_string_view>>&)>
        callback) {
  StartScan();

  for (;;) {
    const auto& row = Iterate();
    if (!row) break;
    callback(*row);
  }
}

}  // namespace cantera
