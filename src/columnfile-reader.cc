#include "columnfile.h"

#include <fcntl.h>
#include <unistd.h>

#include <kj/array.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/io.h>
#include <lz4.h>
#include <lzma.h>
#include <snappy.h>
#include <zlib.h>

#include "columnfile-internal.h"

namespace cantera {

namespace {

using namespace columnfile_internal;

class ColumnFileFdInput : public ColumnFileInput {
 public:
  ColumnFileFdInput(kj::AutoCloseFd fd)
      : fd_{std::move(fd)}, input_{fd_.get()} {
    (void)posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);

    char magic[sizeof(kMagic)];
    input_.tryRead(magic, sizeof(kMagic), sizeof(kMagic));
    KJ_REQUIRE(!memcmp(magic, kMagic, sizeof(kMagic)));
  }

  ~ColumnFileFdInput() override {}

  bool Next(ColumnFileCompression& compression) override;

  std::vector<std::pair<uint32_t, kj::Array<const char>>> Fill(
      const std::unordered_set<uint32_t>& field_filter) override;

  bool End() const override { return end_; }

  void SeekToStart() override {
    KJ_REQUIRE(fd_ != nullptr);

    KJ_SYSCALL(lseek(fd_, sizeof(kMagic), SEEK_SET));

    data_ = {};
    buffer_.clear();
    end_ = false;
  }

  // TODO(mortehu): Implement.
  size_t Size() const override { return 0; }

  // TODO(mortehu): Implement.
  size_t Offset() const override { return 0; }

 private:
  struct FieldMeta {
    uint32_t index;
    uint32_t size;
  };

  bool end_ = false;

  std::string buffer_;

  string_view data_;

  kj::AutoCloseFd fd_;
  kj::FdInputStream input_;

  std::vector<FieldMeta> field_meta_;

  // Set to true when the file position is at the end of the field data.  This
  // means we have to seek backwards if we want to re-read the data.
  bool at_field_end_ = false;
};

class ColumnFileStringInput : public ColumnFileInput {
 public:
  ColumnFileStringInput(string_view data) : input_data_(data) {
    KJ_REQUIRE(input_data_.size() >= sizeof(kMagic));
    KJ_REQUIRE(!memcmp(input_data_.begin(), kMagic, sizeof(kMagic)));
    input_data_.remove_prefix(sizeof(kMagic));

    data_ = input_data_;
  }

  ~ColumnFileStringInput() override {}

  bool Next(ColumnFileCompression& compression) override;

  std::vector<std::pair<uint32_t, kj::Array<const char>>> Fill(
      const std::unordered_set<uint32_t>& field_filter) override;

  bool End() const override { return data_.empty(); }

  void SeekToStart() override { data_ = input_data_; }

  size_t Size() const override { return input_data_.size(); }

  size_t Offset() const override { return input_data_.size() - data_.size(); }

 private:
  struct FieldMeta {
    const char* data;
    uint32_t index;
    uint32_t size;
  };

  string_view input_data_;
  string_view data_;

  std::vector<FieldMeta> field_meta_;
};

bool ColumnFileFdInput::Next(ColumnFileCompression& compression) {
  KJ_REQUIRE(data_.empty());

  uint8_t size_buffer[4];
  const auto ret = input_.tryRead(size_buffer, 4, 4);
  if (ret < 4) {
    end_ = true;
    KJ_REQUIRE(ret == 0);
    return false;
  }

  const uint32_t size = (size_buffer[0] << 24) | (size_buffer[1] << 16) |
                        (size_buffer[2] << 8) | size_buffer[3];
  try {
    buffer_.resize(size);
  } catch (std::bad_alloc e) {
    KJ_FAIL_REQUIRE("Buffer allocation failed", size);
  }
  input_.tryRead(&buffer_[0], size, size);

  data_ = buffer_;

  compression = static_cast<ColumnFileCompression>(GetUInt(data_));

  const auto field_count = GetUInt(data_);

  field_meta_.resize(field_count);

  for (size_t i = 0; i < field_count; ++i) {
    field_meta_[i].index = GetUInt(data_);
    field_meta_[i].size = GetUInt(data_);
  }

  at_field_end_ = false;

  return true;
}

std::vector<std::pair<uint32_t, kj::Array<const char>>> ColumnFileFdInput::Fill(
    const std::unordered_set<uint32_t>& field_filter) {
  std::vector<std::pair<uint32_t, kj::Array<const char>>> result;

  result.reserve(field_filter.empty() ? field_meta_.size()
                                      : field_filter.size());

  if (at_field_end_) {
    off_t reverse_amount = 0;

    for (const auto& f : field_meta_) reverse_amount += f.size;

    KJ_SYSCALL(lseek(fd_, -reverse_amount, SEEK_CUR));
  }

  // Number of bytes to seek before next read.  The purpose of having this
  // variable is to avoid calling lseek several times back-to-back on the
  // same file descriptor.
  size_t skip_amount = 0;

  for (const auto& f : field_meta_) {
    // If the field is ignored, skip its data.
    if (!field_filter.empty() && !field_filter.count(f.index)) {
      skip_amount += f.size;
      continue;
    }

    if (skip_amount > 0) {
      KJ_SYSCALL(lseek(fd_, skip_amount, SEEK_CUR));
      skip_amount = 0;
    }

    auto buffer = kj::heapArray<char>(f.size);
    input_.tryRead(buffer.begin(), f.size, f.size);

    result.emplace_back(f.index, std::move(buffer));
  }

  if (skip_amount > 0) {
    KJ_SYSCALL(lseek(fd_, skip_amount, SEEK_CUR));
  }

  at_field_end_ = true;

  return result;
}

bool ColumnFileStringInput::Next(ColumnFileCompression& compression) {
  KJ_REQUIRE(!data_.empty());

  data_.remove_prefix(4);  // Skip header size we don't need.

  compression = static_cast<ColumnFileCompression>(GetUInt(data_));

  const auto field_count = GetUInt(data_);

  field_meta_.resize(field_count);

  for (size_t i = 0; i < field_count; ++i) {
    field_meta_[i].index = GetUInt(data_);
    field_meta_[i].size = GetUInt(data_);
  }

  for (auto& f : field_meta_) {
    f.data = data_.begin();
    data_.remove_prefix(f.size);
  }

  return true;
}

std::vector<std::pair<uint32_t, kj::Array<const char>>>
ColumnFileStringInput::Fill(const std::unordered_set<uint32_t>& field_filter) {
  std::vector<std::pair<uint32_t, kj::Array<const char>>> result;

  for (const auto& f : field_meta_) {
    if (!field_filter.empty() && !field_filter.count(f.index)) continue;

    // TODO(mortehu): See if we can use a non-owning array instead, e.g.
    //     kj::Array<const char>(data.begin(), f.second,
    //     kj::NullArrayDisposer());
    //  This way, we wouldn't have to copy all the data.

    auto buffer = kj::heapArray<char>(f.data, f.size);

    result.emplace_back(f.index, std::move(buffer));
  }

  return result;
}

}  // namespace

struct ColumnFileReader::Impl {
  class FieldReader {
   public:
    FieldReader(kj::Array<const char> buffer,
                ColumnFileCompression compression);

    FieldReader(FieldReader&&) = default;
    FieldReader& operator=(FieldReader&&) = default;

    KJ_DISALLOW_COPY(FieldReader);

    bool End() const { return !repeat_ && data_.empty(); }

    const string_view* Peek() {
      if (!repeat_) {
        KJ_ASSERT(!data_.empty());
        Fill();
        KJ_ASSERT(repeat_ > 0);
      }

      return value_is_null_ ? nullptr : &value_;
    }

    const string_view* Get() {
      auto result = Peek();
      --repeat_;
      return result;
    }

    void Fill();

   private:
    kj::Array<const char> buffer_;

    string_view data_;

    ColumnFileCompression compression_;

    string_view value_;
    bool value_is_null_ = true;
    uint32_t array_size_ = 0;

    uint32_t repeat_ = 0;
  };

  std::unique_ptr<cantera::columnfile_internal::ThreadPool> thread_pool;

  std::unique_ptr<ColumnFileInput> input;

  std::unordered_set<uint32_t> column_filter;

  ColumnFileCompression compression;

  std::map<uint32_t, FieldReader> fields;

  std::vector<std::pair<uint32_t, optional_string_view>> row_buffer;
};

std::unique_ptr<ColumnFileInput> ColumnFileReader::FileDescriptorInput(
    kj::AutoCloseFd fd) {
  return std::make_unique<ColumnFileFdInput>(std::move(fd));
}

std::unique_ptr<ColumnFileInput> ColumnFileReader::StringInput(
    string_view data) {
  return std::make_unique<ColumnFileStringInput>(data);
}

ColumnFileReader::ColumnFileReader(std::unique_ptr<ColumnFileInput> input)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->input = std::move(input);
}

ColumnFileReader::ColumnFileReader(kj::AutoCloseFd fd)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->input = std::make_unique<ColumnFileFdInput>(std::move(fd));
}

ColumnFileReader::ColumnFileReader(string_view input)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->input = std::make_unique<ColumnFileStringInput>(input);
}

ColumnFileReader::ColumnFileReader(ColumnFileReader&&) = default;

ColumnFileReader::~ColumnFileReader() = default;

void ColumnFileReader::SetColumnFilter(std::unordered_set<uint32_t> columns) {
  pimpl_->column_filter = std::move(columns);
}

bool ColumnFileReader::End() {
  if (!EndOfSegment()) return false;

  if (pimpl_->input->End()) return true;

  Fill();

  return pimpl_->fields.empty();
}

bool ColumnFileReader::EndOfSegment() {
  for (auto i = pimpl_->fields.begin(); i != pimpl_->fields.end();
       i = pimpl_->fields.erase(i)) {
    if (!i->second.End()) return false;
  }

  return true;
}

const string_view* ColumnFileReader::Peek(uint32_t field) {
  for (auto i = pimpl_->fields.begin(); i != pimpl_->fields.end();) {
    if (i->second.End())
      i = pimpl_->fields.erase(i);
    else
      ++i;
  }

  if (pimpl_->fields.empty()) Fill();

  auto i = pimpl_->fields.find(field);
  KJ_REQUIRE(i != pimpl_->fields.end(), "Missing field", field);

  return i->second.Peek();
}

const string_view* ColumnFileReader::Get(uint32_t field) {
  for (auto i = pimpl_->fields.begin(); i != pimpl_->fields.end();) {
    if (i->second.End())
      i = pimpl_->fields.erase(i);
    else
      ++i;
  }

  if (pimpl_->fields.empty()) Fill();

  auto i = pimpl_->fields.find(field);
  KJ_REQUIRE(i != pimpl_->fields.end(), "Missing field", field);

  return i->second.Get();
}

const std::vector<std::pair<uint32_t, optional_string_view>>&
ColumnFileReader::GetRow() {
  pimpl_->row_buffer.clear();

  // TODO(mortehu): This function needs optimization.

  for (auto i = pimpl_->fields.begin(); i != pimpl_->fields.end();) {
    if (i->second.End())
      i = pimpl_->fields.erase(i);
    else
      ++i;
  }

  if (pimpl_->fields.empty()) Fill();

  if (pimpl_->row_buffer.capacity() < pimpl_->fields.size())
    pimpl_->row_buffer.reserve(pimpl_->fields.size());

  for (auto i = pimpl_->fields.begin(); i != pimpl_->fields.end(); ++i) {
    auto data = i->second.Get();

    if (data) {
      pimpl_->row_buffer.emplace_back(i->first, *data);
    } else {
      pimpl_->row_buffer.emplace_back(i->first, nullopt);
    }
  }

  return pimpl_->row_buffer;
}

void ColumnFileReader::SeekToStart() {
  pimpl_->input->SeekToStart();

  pimpl_->fields.clear();
  pimpl_->row_buffer.clear();
}

void ColumnFileReader::SeekToStartOfSegment() {
  pimpl_->fields.clear();
  pimpl_->row_buffer.clear();

  Fill(false);
}

size_t ColumnFileReader::Size() const { return pimpl_->input->Size(); }

size_t ColumnFileReader::Offset() const { return pimpl_->input->Offset(); }

ColumnFileReader::Impl::FieldReader::FieldReader(
    kj::Array<const char> buffer, ColumnFileCompression compression)
    : buffer_{std::move(buffer)},
      data_{buffer_.begin(), buffer_.size()},
      compression_{compression} {}

void ColumnFileReader::Impl::FieldReader::Fill() {
  switch (compression_) {
    case kColumnFileCompressionNone:
      break;

    case kColumnFileCompressionSnappy: {
      size_t decompressed_size = 0;
      KJ_REQUIRE(snappy::GetUncompressedLength(data_.data(), data_.size(),
                                               &decompressed_size));

      auto decompressed_data = kj::heapArray<char>(decompressed_size);
      KJ_REQUIRE(snappy::RawUncompress(data_.data(), data_.size(),
                                       decompressed_data.begin()));
      buffer_ = std::move(decompressed_data);

      data_ = string_view{buffer_.begin(), buffer_.size()};
      compression_ = kColumnFileCompressionNone;
    } break;

    case kColumnFileCompressionLZ4: {
      string_view input(data_);
      auto decompressed_size = GetUInt(input);

      auto decompressed_data = kj::heapArray<char>(decompressed_size);
      auto decompress_result =
          LZ4_decompress_safe(input.data(), decompressed_data.begin(),
                              input.size(), decompressed_size);
      KJ_REQUIRE(decompress_result == static_cast<int>(decompressed_size),
                 decompress_result, decompressed_size);

      buffer_ = std::move(decompressed_data);

      data_ = string_view{buffer_.begin(), buffer_.size()};
      compression_ = kColumnFileCompressionNone;
    } break;

    case kColumnFileCompressionLZMA: {
      string_view input(data_);
      auto decompressed_size = GetUInt(input);

      auto decompressed_data = kj::heapArray<char>(decompressed_size);

      lzma_stream ls = LZMA_STREAM_INIT;

      KJ_REQUIRE(LZMA_OK == lzma_stream_decoder(&ls, UINT64_MAX, 0));

      ls.next_in = reinterpret_cast<const uint8_t*>(input.data());
      ls.avail_in = input.size();
      ls.total_in = input.size();

      ls.next_out = reinterpret_cast<uint8_t*>(decompressed_data.begin());
      ls.avail_out = decompressed_size;

      const auto code_ret = lzma_code(&ls, LZMA_FINISH);
      KJ_REQUIRE(LZMA_STREAM_END == code_ret, code_ret);

      KJ_REQUIRE(ls.total_out == decompressed_size, ls.total_out,
                 decompressed_size);

      buffer_ = std::move(decompressed_data);

      data_ = string_view{buffer_.begin(), buffer_.size()};
      compression_ = kColumnFileCompressionNone;
    } break;

    case kColumnFileCompressionZLIB: {
      string_view input(data_);
      auto decompressed_size = GetUInt(input);

      auto decompressed_data = kj::heapArray<char>(decompressed_size);

      z_stream zs;
      memset(&zs, 0, sizeof(zs));

      KJ_REQUIRE(Z_OK == inflateInit(&zs));
      KJ_DEFER(KJ_REQUIRE(Z_OK == inflateEnd(&zs)));

      zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
      zs.avail_in = input.size();
      zs.total_in = input.size();

      zs.next_out = reinterpret_cast<uint8_t*>(decompressed_data.begin());
      zs.avail_out = decompressed_size;

      const auto inflate_ret = inflate(&zs, LZMA_FINISH);
      KJ_REQUIRE(Z_STREAM_END == inflate_ret, inflate_ret, zs.avail_in,
                 zs.total_in, zs.msg);

      KJ_REQUIRE(zs.total_out == decompressed_size, zs.total_out,
                 decompressed_size);

      buffer_ = std::move(decompressed_data);

      data_ = string_view{buffer_.begin(), buffer_.size()};
      compression_ = kColumnFileCompressionNone;
    } break;

    default:
      KJ_FAIL_REQUIRE("Unknown compression scheme", compression_);
  }

  if (!repeat_) {
    repeat_ = GetUInt(data_);

    const auto reserved = GetUInt(data_);
    KJ_REQUIRE(reserved == 0, reserved);

    auto b0 = static_cast<uint8_t>(data_[0]);

    if ((b0 & 0xc0) == 0xc0) {
      data_.remove_prefix(1);
      if (b0 == kCodeNull) {
        value_is_null_ = true;
      } else {
        // The value we're about to read shares a prefix at least 2 bytes long
        // with the previous value.
        const auto shared_prefix = (b0 & 0x3fU) + 2U;
        const auto suffix_length = GetUInt(data_);

        // Verify that the shared prefix isn't longer than the data we've
        // consumed so far.  If it is, the input is corrupt.
        KJ_REQUIRE(shared_prefix <= value_.size(), shared_prefix,
                   value_.size());

        // We just move the old prefix in front of the new suffix, corrupting
        // whatever data is there; we're not going to read it again anyway.
        memmove(const_cast<char*>(data_.data()) - shared_prefix, value_.begin(),
                shared_prefix);

        value_ = string_view(data_.begin() - shared_prefix,
                             shared_prefix + suffix_length);
        data_.remove_prefix(suffix_length);
        value_is_null_ = false;
      }
    } else {
      auto value_size = GetUInt(data_);
      value_ = string_view(data_.begin(), value_size);
      data_.remove_prefix(value_size);
      value_is_null_ = false;
    }
  }
}

void ColumnFileReader::Fill(bool next) {
  pimpl_->fields.clear();

  if (next && !pimpl_->input->Next(pimpl_->compression)) return;

  auto fields = pimpl_->input->Fill(pimpl_->column_filter);

  KJ_ASSERT(!fields.empty());

  if (pimpl_->compression == kColumnFileCompressionLZMA) {
    if (!pimpl_->thread_pool)
      pimpl_->thread_pool = std::make_unique<ThreadPool>();

    std::vector<std::pair<uint32_t, std::future<Impl::FieldReader>>>
        future_fields;

    for (auto& field : fields) {
      future_fields.emplace_back(field.first, pimpl_->thread_pool->Launch([
        this, data = std::move(field.second)
      ]() mutable {
        Impl::FieldReader result(std::move(data), pimpl_->compression);
        if (!result.End()) result.Fill();
        return result;
      }));
    }

    for (auto& field : future_fields)
      pimpl_->fields.emplace(field.first, field.second.get());
  } else {
    for (auto& field : fields) {
      pimpl_->fields.emplace(
          field.first,
          Impl::FieldReader(std::move(field.second), pimpl_->compression));
    }
  }
}

}  // namespace cantera
