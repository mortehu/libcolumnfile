#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

#include <ext/stdio_filebuf.h>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/sendfile.h>
#include <sysexits.h>
#include <unistd.h>

#include <arpa/inet.h>

#include <kj/arena.h>
#include <kj/debug.h>

#include "columnfile.h"
#include "semaphore.h"

namespace {

using Row = std::vector<std::pair<uint32_t, cantera::optional_string_view>>;

const size_t kFlushLimit = 128 << 20;  // 128 MiB

Semaphore sem_buffer{0};

class StreambufWrapper : public std::streambuf {
 public:
  StreambufWrapper(std::streambuf& underlying) : underlying_{underlying} {}

 protected:
  void imbue(const std::locale& loc) final { underlying_.pubimbue(loc); }

  std::streambuf* setbuf(char_type* s, std::streamsize n) final {
    return underlying_.pubsetbuf(s, n);
  }

  pos_type seekoff(off_type off, std::ios_base::seekdir way,
                   std::ios_base::openmode which) final {
    auto ret = underlying_.pubseekoff(off, way, which);

    // If the seek failed, fall back to reading one character at a time and
    // discarding it.
    if (ret == pos_type(off_type(-1)) && off > 0 && way == std::ios_base::cur &&
        std::ios_base::in == (which & std::ios_base::in)) {
      for (off_type i = 0; i < off; ++i)
        if (traits_type::eof() == underlying_.sbumpc()) return off + i;
      return off;
    }

    return ret;
  }

  pos_type seekpos(pos_type pos, std::ios_base::openmode which) final {
    return underlying_.pubseekpos(pos, which);
  }

  int sync() final { return underlying_.pubsync(); }

  int_type underflow() final { return underlying_.sgetc(); }

  std::streamsize xsgetn(char_type* s, std::streamsize n) final {
    return underlying_.sgetn(s, n);
  }

  std::streamsize xsputn(const char_type* s, std::streamsize n) final {
    return underlying_.sputn(s, n);
  }

  int_type overflow(int_type ch) final { return underlying_.sputc(ch); }

 private:
  std::streambuf& underlying_;
};

size_t GetRow(cantera::ColumnFileReader& reader, kj::Arena* arena,
              Row* output) {
  const auto& row = reader.GetRow();
  size_t fill = 0;

  output->reserve(row.size());
  for (const auto& e : row) {
    if (e.second) {
      auto data = arena->allocateArray<char>(e.second->size());
      std::memcpy(data.begin(), e.second->data(), e.second->size());
      output->emplace_back(e.first,
                           std::string_view{data.begin(), data.size()});
      fill += e.second->size();
    } else {
      output->emplace_back(e.first, std::nullopt);
    }
  }

  fill += row.size() * sizeof((*output)[0]) + sizeof(Row);

  return fill;
}

std::unique_ptr<cantera::ColumnFileInput> Flush(
    std::unique_ptr<kj::Arena> arena, std::vector<Row> rows) {
  std::sort(rows.begin(), rows.end());

  const char* tmpdir = getenv("TMPDIR");
  if (!tmpdir) tmpdir = "/tmp";

  int fd;
  KJ_SYSCALL(fd = open(tmpdir, O_TMPFILE | O_RDWR | O_EXCL, 0777), tmpdir);
  KJ_DEFER(if (fd != -1) close(fd););

  auto streambuf_output = std::make_unique<__gnu_cxx::stdio_filebuf<char>>(
      fd, std::ios::in | std::ios::out | std::ios::binary);
  fd = -1;

  cantera::ColumnFileWriter columnfile_output{std::move(streambuf_output)};

  for (const auto& row : rows) {
    columnfile_output.PutRow(row);
    if (columnfile_output.PendingSize() > kFlushLimit)
      columnfile_output.Flush();
  }

  rows.clear();
  arena.reset();
  sem_buffer.put();

  auto streambuf_input = columnfile_output.Finalize();
  streambuf_input->pubseekpos(0);

  return cantera::ColumnFileReader::StreambufInput(std::move(streambuf_input));
}

std::mutex mutex_flush;

std::future<std::unique_ptr<cantera::ColumnFileInput>> FlushAsync(
    std::unique_ptr<kj::Arena> arena, std::vector<Row>&& rows) {
  return std::async(std::launch::async, Flush, std::move(arena),
                    std::move(rows));
}

void Merge(std::deque<std::future<std::unique_ptr<cantera::ColumnFileInput>>>
               batch_inputs, cantera::ColumnFileWriter& output) {
  using QueueElement = std::pair<Row, cantera::ColumnFileReader>;

  struct QueueCompare {
    bool operator()(const QueueElement& lhs, const QueueElement& rhs) const {
      return lhs.first > rhs.first;
    }
  };

  std::vector<QueueElement> queue;

  for (auto&& future_input : batch_inputs) {
    cantera::ColumnFileReader reader{future_input.get()};
    if (!reader.End()) {
      auto&& row = reader.GetRow();
      queue.emplace_back(std::move(row), std::move(reader));
    }
  }

  QueueCompare cmp;

  std::make_heap(queue.begin(), queue.end(), cmp);

  while (queue.size() > 1) {
    std::pop_heap(queue.begin(), queue.end(), cmp);

    auto& top = queue.back();
    output.PutRow(top.first);

    if (output.PendingSize() > kFlushLimit) output.Flush();

    if (top.second.End()) {
      queue.pop_back();
      continue;
    }

    top.first = top.second.GetRow();
    std::push_heap(queue.begin(), queue.end(), cmp);
  }

  // Fast path for when there's only one input stream left.

  KJ_ASSERT(queue.size() == 1);

  output.PutRow(queue.back().first);

  auto& input = queue.back().second;

  while (!input.End()) {
    output.PutRow(input.GetRow());
    if (output.PendingSize() > kFlushLimit) output.Flush();
  }
}

std::future<std::unique_ptr<cantera::ColumnFileInput>> Merge(std::deque<
    std::future<std::unique_ptr<cantera::ColumnFileInput>>> batch_inputs) {
  const char* tmpdir = getenv("TMPDIR");
  if (!tmpdir) tmpdir = "/tmp";

  int fd;
  KJ_SYSCALL(fd = open(tmpdir, O_TMPFILE | O_RDWR | O_EXCL, 0777), tmpdir);
  KJ_DEFER(if (fd != -1) close(fd););

  auto streambuf_output = std::make_unique<__gnu_cxx::stdio_filebuf<char>>(
      fd, std::ios::in | std::ios::out | std::ios::binary);
  fd = -1;

  cantera::ColumnFileWriter columnfile_output{std::move(streambuf_output)};

  Merge(std::move(batch_inputs), columnfile_output);

  auto streambuf_input = columnfile_output.Finalize();
  streambuf_input->pubseekpos(0);

  std::promise<std::unique_ptr<cantera::ColumnFileInput>> result;
  result.set_value(
      cantera::ColumnFileReader::StreambufInput(std::move(streambuf_input)));
  return result.get_future();
}

int do_merge = 0;
int print_version = 0;
int print_help = 0;

enum Option {
  kOptionBufferSize = 'B',
};

struct option kLongOptions[] = {
    {"buffer-size", required_argument, nullptr, kOptionBufferSize},
    {"merge", no_argument, &do_merge, 1},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {nullptr, 0, nullptr, 0}};

const std::string kCasTableUrnPrefix = "urn:ca-cas-table:";

}  // namespace

int main(int argc, char** argv) try {
  size_t buffer_size = 1024;

  int i;
  while ((i = getopt_long(argc, argv, "na:p:", kLongOptions, 0)) != -1) {
    if (!i) continue;
    if (i == '?')
      errx(EX_USAGE, "Try '%s --help' for more information.", argv[0]);

    switch (i) {
      case kOptionBufferSize:
        buffer_size = std::stoull(optarg);
        break;
    }
  }

  if (print_help) {
    printf(
        "Usage: %s [OPTION]... [FILE]...\n"
        "\n"
        "      --buffer-size=SIZE     buffer size in megabytes [%zu]\n"
        "      --merge                merge already sorted files\n"
        "      --help     display this help and exit\n"
        "      --version  display version information and exit\n"
        "\n"
        "With no FILE, or when FILE is -, read standard input.\n"
        "\n"
        "Report bugs to <morten.hustveit@gmail.com>\n",
        argv[0], buffer_size);

    return EXIT_SUCCESS;
  }

  if (print_version) {
    puts(PACKAGE_STRING);

    return EXIT_SUCCESS;
  }

  buffer_size <<= 20;  // Convert to megabytes.

  sem_buffer.put(std::thread::hardware_concurrency());

#if HAVE_CA_CAS
  auto aio_context = kj::setupAsyncIo();
  std::unique_ptr<cantera::CASClient> cas_client;
#endif

  std::vector<std::unique_ptr<cantera::ColumnFileInput>> inputs;

  if (optind == argc) {
    inputs.emplace_back(cantera::ColumnFileReader::StreambufInput(
        std::make_unique<StreambufWrapper>(*std::cin.rdbuf())));
  } else {
    for (auto i = optind; i < argc; ++i) {
      std::string path{argv[i]};
      if (path == "-") {
        inputs.emplace_back(cantera::ColumnFileReader::StreambufInput(
            std::make_unique<StreambufWrapper>(*std::cin.rdbuf())));
      } else if (0 ==
                 path.compare(0, kCasTableUrnPrefix.size(),
                              kCasTableUrnPrefix)) {
#if HAVE_CA_CAS
        if (!cas_client)
          cas_client = std::make_unique<cantera::CASClient>(aio_context);

        inputs.emplace_back(std::make_unique<cantera::CASColumnFileInput>(
            cas_client.get(), path.substr(kCasTableUrnPrefix.size())));
#else
        errx(EXIT_FAILURE, "CAS files not supported by this build");
#endif
      } else {
        auto filebuf = std::make_unique<std::filebuf>();
        KJ_REQUIRE(nullptr !=
                       filebuf->open(argv[i],
                                     std::ios_base::binary | std::ios_base::in),
                   argv[i]);
        inputs.emplace_back(
            cantera::ColumnFileReader::StreambufInput(std::move(filebuf)));
      }
    }
  }

  std::deque<std::future<std::unique_ptr<cantera::ColumnFileInput>>>
      merge_inputs;

  if (do_merge) {
    for (auto& input : inputs) {
      std::promise<std::unique_ptr<cantera::ColumnFileInput>> promise;
      merge_inputs.emplace_back(promise.get_future());
      promise.set_value(std::move(input));
    }
  } else {
    sem_buffer.get();
    std::unique_ptr<kj::Arena> arena = std::make_unique<kj::Arena>();
    std::vector<Row> rows;
    size_t fill = 0;

    for (auto& input : inputs) {
      cantera::ColumnFileReader reader(std::move(input));

      while (!reader.End()) {
        rows.emplace_back();
        fill += GetRow(reader, arena.get(), &rows.back());

        if (fill >= buffer_size) {
          merge_inputs.emplace_back(
              FlushAsync(std::move(arena), std::move(rows)));

          sem_buffer.get();
          rows.clear();
          arena = std::make_unique<kj::Arena>();
          fill = 0;
        }
      }
    }

    if (!rows.empty())
      merge_inputs.emplace_back(FlushAsync(std::move(arena), std::move(rows)));
  }

  inputs.clear();

  const size_t kBatchSize = 8;

  while (merge_inputs.size() > kBatchSize) {
    std::deque<std::future<std::unique_ptr<cantera::ColumnFileInput>>>
        batch_inputs;

    while (merge_inputs.size() + 1 > kBatchSize &&
           batch_inputs.size() < kBatchSize) {
      batch_inputs.emplace_back(std::move(merge_inputs.front()));
      merge_inputs.pop_front();
    }

    merge_inputs.emplace_back(Merge(std::move(batch_inputs)));
  }

  cantera::ColumnFileWriter output(
      std::make_unique<StreambufWrapper>(*std::cout.rdbuf()));

  Merge(std::move(merge_inputs), output);
  output.Finalize();
} catch (kj::Exception& e) {
  KJ_LOG(FATAL, e);
  return EXIT_FAILURE;
} catch (std::runtime_error& e) {
  fprintf(stderr, "Runtime error: %s\n", e.what());
  return EXIT_FAILURE;
}
