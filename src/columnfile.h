#ifndef CANTERA_COLUMNFILE_H_
#define CANTERA_COLUMNFILE_H_ 1

#include <cstdint>
#include <experimental/optional>
#include <experimental/string_view>
#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include <kj/array.h>
#include <kj/io.h>

namespace cantera {

using string_view = std::experimental::string_view;
using optional_string_view = std::experimental::optional<string_view>;
constexpr auto nullopt = std::experimental::nullopt;

enum ColumnFileCompression : uint32_t {
  // No compression.
  kColumnFileCompressionNone = 0,

  // Snappy compression.  https://google.github.io/snappy/
  kColumnFileCompressionSnappy = 1,

  // LZ4 compression.  https://github.com/Cyan4973/lz4
  kColumnFileCompressionLZ4 = 2,

  // Lempel-Ziv-Markov chain algorithm.  This is algorithm is by far the
  // slowest among the ones provided, but also generates the smallest output.
  kColumnFileCompressionLZMA = 3,

  // Deflate compression.  Compression is multi-threaded, while decompression
  // is not.
  kColumnFileCompressionZLIB = 4,

  kColumnFileCompressionDefault = kColumnFileCompressionLZ4
};

// Interface used to implement new column file storage methods.
class ColumnFileOutput {
 public:
  virtual ~ColumnFileOutput() noexcept(false) {}

  // Writes a new chunk to the underlying storage.  The `fields` parameter
  // contains the per-column compressed and encoded data, while the
  // `compression` parameter indicates what compression algorithm was used.
  //
  // The underlying storage backend needs to remember the compression method
  // used for each chunk.
  virtual void Flush(
      const std::vector<std::pair<uint32_t, string_view>>& fields,
      const ColumnFileCompression compression) = 0;

  // Finishes writing the file.  Returns the underlying file descriptor, if
  // available.
  virtual kj::AutoCloseFd Finalize() = 0;
};

class ColumnFileWriter {
 public:
  static ColumnFileCompression StringToCompressingAlgorithm(
      const string_view& name);

  explicit ColumnFileWriter(std::shared_ptr<ColumnFileOutput> output);

  explicit ColumnFileWriter(kj::AutoCloseFd&& fd);

  explicit ColumnFileWriter(std::string& output);

  ~ColumnFileWriter();

  // Sets the compression algorithm to use for future blocks.
  void SetCompression(ColumnFileCompression c);

  // Inserts a value.
  void Put(uint32_t column, const string_view& data);
  void PutNull(uint32_t column);

  void PutRow(
      const std::vector<std::pair<uint32_t, optional_string_view>>& row);

  // Returns an approximate number of uncompressed bytes that have not yet been
  // flushed.  This can be used to make a decision as to whether or not to call
  // `Flush()`.
  size_t PendingSize() const;

  // Writes all buffered records to the output stream.
  void Flush();

  // Finishes writing the file.  Returns the underlying file descriptor.
  //
  // This function is implicitly called by the destructor.
  kj::AutoCloseFd Finalize();

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl_;
};

class ColumnFileInput {
 public:
  virtual ~ColumnFileInput() noexcept(false) {}

  // Moves to the next segment.  Returns `false` if the end of the input stream
  // was reached, `true` otherwise.
  virtual bool Next(ColumnFileCompression& compression) = 0;

  // Returns the data chunks for the fields specified in `field_filter`.  If
  // `field_filter` is empty, all fields are selected.
  virtual std::vector<std::pair<uint32_t, kj::Array<const char>>> Fill(
      const std::unordered_set<uint32_t>& field_filter) = 0;

  // Returns `true` if the next call to `Fill` will definitely return an
  // empty vector, `false` otherwise.
  virtual bool End() const = 0;

  // Seek to the beginning of the input.
  virtual void SeekToStart() = 0;

  // Returns the size of the input, in an unspecified unit.
  virtual size_t Size() const = 0;

  // Returns the approximate offset, in an unspecified unit.  This value only
  // makes sense when compared to the return value of `Size()`.
  virtual size_t Offset() const = 0;
};

class ColumnFileReader {
 public:
  static std::unique_ptr<ColumnFileInput> FileDescriptorInput(
      kj::AutoCloseFd fd);

  static std::unique_ptr<ColumnFileInput> StringInput(string_view data);

  explicit ColumnFileReader(std::unique_ptr<ColumnFileInput> input);

  // Reads a column file as a stream.  If you want to use memory-mapped I/O,
  // use the string_view based constructor below.
  explicit ColumnFileReader(kj::AutoCloseFd fd);

  // Reads a column file from memory.
  explicit ColumnFileReader(string_view input);

  ColumnFileReader(ColumnFileReader&&);

  ColumnFileReader& operator=(ColumnFileReader&&) = default;

  ~ColumnFileReader();

  void SetColumnFilter(std::unordered_set<uint32_t> columns);

  template <typename Iterator>
  void SetColumnFilter(Iterator begin, Iterator end) {
    std::unordered_set<uint32_t> tmp;
    while (begin != end) tmp.emplace(*begin++);
    SetColumnFilter(std::move(tmp));
  }

  // Returns true iff there's no more data to be read.
  bool End();

  // Returns true iff there's no more data to be read in the current segment.
  bool EndOfSegment();

  // Returns the current value for the given column.
  const string_view* Peek(uint32_t field);

  // Returns the current value for the given column and advanced its read pointer.
  const string_view* Get(uint32_t field);

  const std::vector<std::pair<uint32_t, optional_string_view>>& GetRow();

  void SeekToStart();

  void SeekToStartOfSegment();

  size_t Size() const;

  size_t Offset() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl_;

  void Fill(bool next = true);
};

// Wraps `ColumnFileReader` to provide efficient querying of column tables.
// The requested columns are selectively retrieved, decompressed, and decoded
// based on whether the provided filters match any rows in each segment.
//
// As an example, imagine a table with two columns.  A filter is added for the
// first column, returning true iff the value is "1".  As a result, the entire
// first column will be decoded, but chunks of the second column will be
// skipped if the chunk does not have any rows whose value in the first column
// is "1".
class ColumnFileSelect {
 public:
  ColumnFileSelect(ColumnFileReader input);

  ~ColumnFileSelect();

  // Adds a column to be returned by `Iterate()`.
  void AddSelection(uint32_t field);

  // Adds a filter to limit the results returned by `Iterate()`, and to
  // eliminate unnecessary reading and decoding of non-matching chunks.
  void AddFilter(uint32_t field,
                 std::function<bool(const optional_string_view&)> filter);

  // Must be called before the first call to `Iterate()`.
  void StartScan();

  // Returns the next row, if any.  Use `KJ_IF_MAYBE()` to process the result.
  kj::Maybe<const std::vector<std::pair<uint32_t, optional_string_view>>&>
  Iterate();

  // Convenience wrapper for `StartScan()` and `Iterate()`.
  void Execute(
      std::function<
          void(const std::vector<std::pair<uint32_t, optional_string_view>>&)>
          callback);

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl_;

  void ReadChunk();
};

}  // namespace cantera

#endif  // !CANTERA_COLUMNFILE_H_
