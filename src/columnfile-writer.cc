#include "columnfile.h"

#include <cassert>

#include <lz4.h>
#include <lzma.h>
#include <snappy.h>

#include "columnfile-internal.h"

namespace cantera {

namespace {

using namespace columnfile_internal;

class ColumnFileStreambufOutput : public ColumnFileOutput {
 public:
  ColumnFileStreambufOutput(std::unique_ptr<std::streambuf>&& fd);

  void Flush(const std::vector<std::pair<uint32_t, std::string_view>>& fields,
             const ColumnFileCompression compression) override;

  std::unique_ptr<std::streambuf> Finalize() override { return std::move(fd_); }

 private:
  std::unique_ptr<std::streambuf> fd_;
};

class ColumnFileStringOutput : public ColumnFileOutput {
 public:
  ColumnFileStringOutput(std::string& output) : output_{output} {
    if (output_.empty()) output_.append(kMagic, sizeof(kMagic));
  }

  void Flush(const std::vector<std::pair<uint32_t, std::string_view>>& fields,
             const ColumnFileCompression compression) override;

  std::unique_ptr<std::streambuf> Finalize() override { return {}; }

 private:
  std::string& output_;
};

ColumnFileStreambufOutput::ColumnFileStreambufOutput(
    std::unique_ptr<std::streambuf>&& fd)
    : fd_{std::move(fd)} {
  const auto offset = fd_->pubseekoff(0, std::ios_base::end);
  if (offset <= 0) {
    if (sizeof(kMagic) != fd_->sputn(kMagic, sizeof(kMagic)))
      throw ColumnFileException{"sputn() failed to write magic bytes"};
  }
}

void ColumnFileStreambufOutput::Flush(
    const std::vector<std::pair<uint32_t, std::string_view>>& fields,
    const ColumnFileCompression compression) {
  std::string buffer;
  buffer.resize(4, 0);

  PutUInt(buffer, compression);
  PutUInt(buffer, fields.size());

  for (auto& field : fields) {
    PutUInt(buffer, field.first);
    PutUInt(buffer, field.second.size());
  }

  auto buffer_size = buffer.size() - 4;  // Don't count the size itself.
  buffer[0] = buffer_size >> 24U;
  buffer[1] = buffer_size >> 16U;
  buffer[2] = buffer_size >> 8U;
  buffer[3] = buffer_size;

  if (static_cast<std::streamsize>(buffer.size()) !=
      fd_->sputn(buffer.data(), buffer.size()))
    throw ColumnFileException{"sputn() failed to write segment metadata"};

  for (const auto& field : fields)
    if (static_cast<std::streamsize>(field.second.size()) !=
        fd_->sputn(field.second.data(), field.second.size()))
      throw ColumnFileException{"sputn() failed to write segment"};

  if (0 != fd_->pubsync()) throw ColumnFileException{"pubsync() failed"};
}

void ColumnFileStringOutput::Flush(
    const std::vector<std::pair<uint32_t, std::string_view>>& fields,
    const ColumnFileCompression compression) {
  std::string buffer;
  buffer.resize(4, 0);

  PutUInt(buffer, compression);
  PutUInt(buffer, fields.size());

  for (auto& field : fields) {
    PutUInt(buffer, field.first);
    PutUInt(buffer, field.second.size());
  }

  auto buffer_size = buffer.size() - 4;  // Don't count the size itself.
  buffer[0] = buffer_size >> 24U;
  buffer[1] = buffer_size >> 16U;
  buffer[2] = buffer_size >> 8U;
  buffer[3] = buffer_size;

  output_ += buffer;

  for (const auto& field : fields)
    output_.append(field.second.begin(), field.second.end());
}

}  // namespace

struct ColumnFileWriter::Impl {
  class FieldWriter {
   public:
    void Put(const std::string_view& data);

    void PutNull();

    void Flush();

    void Finalize(ColumnFileCompression compression);

    std::string_view Data() const { return data_; }

   private:
    std::string data_;

    std::string value_;
    bool value_is_null_ = false;

    uint32_t repeat_ = 0;

    unsigned int shared_prefix_ = 0;
  };

  std::shared_ptr<ColumnFileOutput> output;

  ColumnFileCompression compression = kColumnFileCompressionDefault;

  std::map<uint32_t, FieldWriter> fields;

  size_t pending_size = 0;
};

ColumnFileCompression ColumnFileWriter::StringToCompressingAlgorithm(
    const std::string_view& name) {
  if (name == "none") return kColumnFileCompressionNone;
  if (name == "snappy") return kColumnFileCompressionSnappy;
  if (name == "lz4") return kColumnFileCompressionLZ4;
  if (name == "lzma") return kColumnFileCompressionLZMA;
  if (name == "zlib") return kColumnFileCompressionZLIB;
  throw ColumnFileException{"unsupported compression algorithm"};
}

ColumnFileWriter::ColumnFileWriter(std::shared_ptr<ColumnFileOutput> output)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->output = std::move(output);
}

ColumnFileWriter::ColumnFileWriter(std::unique_ptr<std::streambuf>&& fd)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->output = std::make_shared<ColumnFileStreambufOutput>(std::move(fd));
}

ColumnFileWriter::ColumnFileWriter(std::string& output)
    : pimpl_{std::make_unique<Impl>()} {
  pimpl_->output = std::make_shared<ColumnFileStringOutput>(output);
}

ColumnFileWriter::ColumnFileWriter(ColumnFileWriter&& rhs) = default;

ColumnFileWriter& ColumnFileWriter::operator=(ColumnFileWriter&& rhs) {
  Finalize();
  pimpl_ = std::move(rhs.pimpl_);
  return *this;
}

ColumnFileWriter::~ColumnFileWriter() { Finalize(); }

void ColumnFileWriter::SetCompression(ColumnFileCompression c) {
  pimpl_->compression = c;
}

void ColumnFileWriter::Put(uint32_t column, const std::string_view& data) {
  pimpl_->fields[column].Put(data);
  pimpl_->pending_size += data.size();
}

void ColumnFileWriter::PutNull(uint32_t column) {
  pimpl_->fields[column].PutNull();
  ++pimpl_->pending_size;
}

void ColumnFileWriter::PutRow(
    const std::vector<std::pair<uint32_t, optional_string_view>>& row) {
  // We iterate simultaneously through the pimpl_->fields map and the row, so
  // that if their keys matches, we don't have to perform any binary searches
  // in the map.
  auto field_it = pimpl_->fields.begin();
  auto row_it = row.begin();

  while (row_it != row.end()) {
    if (field_it == pimpl_->fields.end() || field_it->first != row_it->first) {
      field_it = pimpl_->fields.find(row_it->first);
      if (field_it == pimpl_->fields.end())
        field_it =
            pimpl_->fields.emplace(row_it->first, Impl::FieldWriter{}).first;
    }

    if (!row_it->second) {
      field_it->second.PutNull();
    } else {
      const auto& str = row_it->second.value();
      field_it->second.Put(str);
      pimpl_->pending_size += str.size();
    }

    ++row_it;
    ++field_it;
  }
}

size_t ColumnFileWriter::PendingSize() const { return pimpl_->pending_size; }

void ColumnFileWriter::Flush() {
  if (pimpl_->fields.empty()) return;

  std::vector<std::pair<uint32_t, std::string_view>> field_data;
  field_data.reserve(pimpl_->fields.size());

  for (auto& field : pimpl_->fields) {
    field.second.Finalize(pimpl_->compression);
    field_data.emplace_back(field.first, field.second.Data());
  }

  pimpl_->output->Flush(field_data, pimpl_->compression);

  pimpl_->fields.clear();

  pimpl_->pending_size = 0;
}

std::unique_ptr<std::streambuf> ColumnFileWriter::Finalize() {
  if (!pimpl_ || !pimpl_->output) return {};
  Flush();
  auto result = pimpl_->output->Finalize();
  pimpl_->output.reset();
  return result;
}

void ColumnFileWriter::Impl::FieldWriter::Put(const std::string_view& data) {
  bool data_mismatch;
  unsigned int shared_prefix = 0;
  if (value_is_null_) {
    data_mismatch = true;
  } else {
    auto i =
        std::mismatch(data.begin(), data.end(), value_.begin(), value_.end());
    if (i.first != data.end() || i.second != value_.end()) {
      shared_prefix = std::distance(data.begin(), i.first);
      data_mismatch = true;
    } else {
      data_mismatch = false;
    }
  }

  if (data_mismatch) {
    Flush();
    if (data_mismatch) {
      value_.assign(data.begin(), data.end());
      value_is_null_ = false;
      shared_prefix_ = shared_prefix;
    }
  }

  ++repeat_;
}

void ColumnFileWriter::Impl::FieldWriter::PutNull() {
  if (!value_is_null_) Flush();

  value_is_null_ = true;
  ++repeat_;
}

void ColumnFileWriter::Impl::FieldWriter::Flush() {
  if (!repeat_) return;

  PutUInt(data_, repeat_);
  PutUInt(data_, 0);  // Reserved field.

  if (value_is_null_) {
    data_.push_back(kCodeNull);
  } else {
    if (shared_prefix_ > 2) {
      // Make sure we don't produce 0xff in the output, which is used to
      // indicate NULL values.
      if (shared_prefix_ > 0x40) shared_prefix_ = 0x40;
      data_.push_back(0xc0 | (shared_prefix_ - 2));
      PutUInt(data_, value_.size() - shared_prefix_);
      data_.append(value_.begin() + shared_prefix_, value_.end());
    } else {
      PutUInt(data_, value_.size());
      data_.append(value_.begin(), value_.end());
    }
  }

  repeat_ = 0;
  value_is_null_ = true;
}

void ColumnFileWriter::Impl::FieldWriter::Finalize(
    ColumnFileCompression compression) {
  Flush();

  switch (compression) {
    case kColumnFileCompressionNone:
      break;

    case kColumnFileCompressionSnappy: {
      std::string compressed_data;
      compressed_data.resize(snappy::MaxCompressedLength(data_.size()));
      size_t compressed_length = SIZE_MAX;
      snappy::RawCompress(data_.data(), data_.size(), &compressed_data[0],
                          &compressed_length);
      assert(compressed_length <= compressed_data.size());
      compressed_data.resize(compressed_length);
      data_.swap(compressed_data);
    } break;

    case kColumnFileCompressionLZ4: {
      std::string compressed_data;
      PutUInt(compressed_data, data_.size());
      const auto data_offset = compressed_data.size();
      compressed_data.resize(data_offset + LZ4_compressBound(data_.size()));

      const auto compressed_length = LZ4_compress(
          data_.data(), &compressed_data[data_offset], data_.size());
      assert(data_offset + compressed_length <= compressed_data.size());
      compressed_data.resize(data_offset + compressed_length);
      data_.swap(compressed_data);
    } break;

    case kColumnFileCompressionLZMA: {
      std::string compressed_data;
      PutUInt(compressed_data, data_.size());
      const auto data_offset = compressed_data.size();
      compressed_data.resize(data_offset +
                             lzma_stream_buffer_bound(data_.size()));

      lzma_stream ls = LZMA_STREAM_INIT;

      if (LZMA_OK != lzma_easy_encoder(&ls, 1, LZMA_CHECK_CRC32))
        throw ColumnFileException{"lzma_easy_encoder() did not return LZMA_OK"};

      ls.next_in = reinterpret_cast<const uint8_t*>(data_.data());
      ls.avail_in = data_.size();
      ls.total_in = data_.size();

      ls.next_out = reinterpret_cast<uint8_t*>(&compressed_data[data_offset]);
      ls.avail_out = compressed_data.size() - data_offset;

      const auto code_ret = lzma_code(&ls, LZMA_FINISH);
      if (LZMA_STREAM_END != code_ret)
        throw ColumnFileException{
            "lzma_code(..., LZMA_FINISH) did not return LZMA_STREAM_END"};

      const auto compressed_length = ls.total_out;
      assert(data_offset + compressed_length <= compressed_data.size());

      lzma_end(&ls);

      compressed_data.resize(data_offset + compressed_length);
      data_.swap(compressed_data);
    } break;

    case kColumnFileCompressionZLIB: {
      std::string compressed_data;
      PutUInt(compressed_data, data_.size());

      CompressZLIB(compressed_data, data_);

      data_.swap(compressed_data);
    } break;

    default:
      throw ColumnFileException{"unknown compression scheme"};
  }
}

}  // namespace cantera
