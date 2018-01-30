#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/sendfile.h>
#include <sysexits.h>
#include <unistd.h>

#if HAVE_CA_CAS
#include <ca-cas/cas-columnfile.h>
#include <ca-cas/client.h>
#endif

#include <columnfile.h>
#include <kj/debug.h>

namespace {

int print_version = 0;
int print_help = 0;
cantera::ColumnFileCompression compression =
    cantera::kColumnFileCompressionDefault;
std::string format;
std::string output_format;
std::vector<std::pair<uint32_t, std::string>> filters;

enum Option {
  kOptionCompression = 'c',
  kOptionFilter = 'F',
  kOptionFormat = 'f',
  kOptionOutputFormat = 'o'
};

const std::string kCasTableUrnPrefix = "urn:ca-cas-table:";

const size_t kFlushLimit(16 << 20);  // 16 MiB

struct option kLongOptions[] = {
    {"compression", required_argument, nullptr, kOptionCompression},
    {"filter", required_argument, nullptr, kOptionFilter},
    {"format", required_argument, nullptr, kOptionFormat},
    {"output-format", required_argument, nullptr, kOptionOutputFormat},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {nullptr, 0, nullptr, 0}};

const std::map<std::string, cantera::ColumnFileCompression> kCompressionMethods{
    {
        {"none", cantera::kColumnFileCompressionNone},
        {"snappy", cantera::kColumnFileCompressionSnappy},
        {"lz4", cantera::kColumnFileCompressionLZ4},
        {"lzma", cantera::kColumnFileCompressionLZMA},
        {"deflate", cantera::kColumnFileCompressionZLIB},
    }};

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

}  // namespace

int main(int argc, char** argv) try {
  int i;
  while ((i = getopt_long(argc, argv, "f:", kLongOptions, 0)) != -1) {
    if (!i) continue;
    if (i == '?')
      errx(EX_USAGE, "Try '%s --help' for more information.", argv[0]);

    switch (static_cast<Option>(i)) {
      case kOptionCompression:
        try {
          compression = kCompressionMethods.at(optarg);
        } catch (std::out_of_range&) {
          errx(EX_USAGE, "Unknown compression algorithm '%s'", optarg);
        }
        break;

      case kOptionFilter: {
        auto delimiter = strchr(optarg, ':');
        KJ_REQUIRE(delimiter != nullptr, optarg);
        *delimiter = 0;
        filters.emplace_back(std::stoull(optarg), delimiter + 1);
      } break;

      case kOptionFormat:
        format = optarg;
        break;

      case kOptionOutputFormat:
        output_format = optarg;
        break;
    }
  }

  if (print_help) {
    printf(
        "Usage: %s [OPTION]... [FILE]...\n"
        "\n"
        "      --compression=METHOD   select output compression method\n"
        "      --format=FORMAT        column formats\n"
        "      --filter=COL:PATTERN   only show rows whose COLUMN matches "
        "PATTERN\n"
        "      --output-format=TYPE   select output data format\n"
        "      --help     display this help and exit\n"
        "      --version  display version information and exit\n"
        "\n"
        "With no FILE, or when FILE is -, read standard input.\n"
        "\n"
        "Data formats supported by the --output-format option:\n"
        "    text        Tab delimited values\n"
        "    columnfile  Column file (e.g. for recompression)\n"
        "\n"
        "Report bugs to <morten.hustveit@gmail.com>\n",
        argv[0]);

    return EXIT_SUCCESS;
  }

  if (print_version) {
    puts(PACKAGE_STRING);

    return EXIT_SUCCESS;
  }

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
      } else if (0 == path.compare(0, kCasTableUrnPrefix.size(),
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
        KJ_REQUIRE(nullptr != filebuf->open(argv[i], std::ios_base::binary |
                                                         std::ios_base::in),
                   argv[i]);
        inputs.emplace_back(
            cantera::ColumnFileReader::StreambufInput(std::move(filebuf)));
      }
    }
  }

  std::unordered_set<uint32_t> selected_fields;

  if (!format.empty()) {
    for (uint32_t i = 0; i < format.size(); ++i) {
      if (format[i] != '_') selected_fields.emplace(i);
    }
  }

  for (const auto& filter : filters) selected_fields.emplace(filter.first);

  std::sort(filters.begin(), filters.end());

  if (output_format.empty() || output_format == "text") {
    for (auto& input : inputs) {
      cantera::ColumnFileReader reader(std::move(input));

      if (!selected_fields.empty())
        reader.SetColumnFilter(selected_fields.begin(), selected_fields.end());

      while (!reader.End()) {
        const auto& row = reader.GetRow();
        size_t column = 0;

        if (!filters.empty()) {
          auto filter = filters.begin();
          auto field = row.begin();

          while (filter != filters.end() && field != row.end()) {
            if (field->first < filter->first) {
              ++field;
              continue;
            }

            if (filter->first < field->first) break;

            if (std::string::npos == field->second.value().find(filter->second))
              break;

            ++field;
            ++filter;
          }

          if (filter != filters.end()) continue;
        }

        bool need_tab = false;

        for (const auto& field : row) {
          auto fmt = 's';

          if (!format.empty()) {
            if (field.first >= format.size()) break;

            fmt = format[field.first];
          }

          while (column < field.first) {
            if (format[column++] != '_') std::cout << '\t';
          }

          if (fmt == '_') continue;

          if (need_tab) std::cout << '\t';

          switch (fmt) {
#define FMT(ch, type)                                       \
  case ch: {                                                \
    KJ_REQUIRE(field.second.value().size() >= sizeof(type), \
               field.second.value().size(), fmt);           \
    type f;                                                 \
    memcpy(&f, field.second.value().data(), sizeof(f));     \
    std::cout << f;                                         \
    need_tab = true;                                        \
  } break;

            // Based on Python's "struct" module format characters.
            FMT('H', uint16_t)
            FMT('I', uint32_t)
            FMT('Q', uint64_t)
            FMT('d', double)
            FMT('f', float)
            FMT('h', int16_t)
            FMT('i', int32_t)
            FMT('q', int64_t)

#undef FMT

            case 's':
              std::cout << field.second.value();
              need_tab = true;
              break;

            case 'x':
              std::cout << std::hex << std::setfill('0');
              for (const auto ch : field.second.value())
                std::cout << std::setw(2) << static_cast<int>(static_cast<uint8_t>(ch));
              std::cout << std::dec;
              need_tab = true;
              break;
          }

          ++column;
        }

        std::cout << '\n';
      }
    }
  } else if (output_format == "columnfile") {
    cantera::ColumnFileWriter output(
        std::make_unique<StreambufWrapper>(*std::cout.rdbuf()));
    output.SetCompression(compression);

    for (auto& input : inputs) {
      cantera::ColumnFileReader reader(std::move(input));

      while (!reader.End()) {
        output.PutRow(reader.GetRow());

        if (output.PendingSize() > kFlushLimit) output.Flush();
      }
    }
#if HAVE_CA_CAS
  } else if (output_format == "cas-columnfile") {
    if (!cas_client)
      cas_client = std::make_unique<cantera::CASClient>(aio_context);
    auto cas_output =
        std::make_shared<cantera::CASColumnFileOutput>(cas_client.get());

    cantera::ColumnFileWriter output{cas_output};
    output.SetCompression(compression);

    for (auto& input : inputs) {
      cantera::ColumnFileReader reader(std::move(input));

      while (!reader.End()) {
        output.PutRow(reader.GetRow());

        if (output.PendingSize() > kFlushLimit) output.Flush();
      }
    }

    output.Finalize();

    std::cout << kCasTableUrnPrefix << cas_output->Key() << '\n';
#endif
  } else {
    KJ_FAIL_REQUIRE("Unknown output format", output_format);
  }
} catch (kj::Exception& e) {
  KJ_LOG(FATAL, e);
  return EXIT_FAILURE;
} catch (std::runtime_error& e) {
  fprintf(stderr, "Runtime error: %s\n", e.what());
  return EXIT_FAILURE;
}
