#include <cstring>

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>

#include <capnp/schema-parser.h>
#include <capnp/serialize.h>

#include "columnfile-capnp.h"
#include "columnfile-internal.h"
#include "columnfile.h"
#include "third_party/gtest/gtest.h"

using namespace cantera;

namespace {

inline bool HasSuffix(const std::string_view& haystack,
                      const std::string_view& needle) {
  if (haystack.size() < needle.size()) return false;
  return 0 == std::memcmp(haystack.data() + haystack.size() - needle.size(),
                          needle.data(), needle.size());
}

std::unique_ptr<std::streambuf> OpenFile(const char* path,
                                         std::ios_base::openmode mode) {
  auto result = std::make_unique<std::filebuf>();
  KJ_REQUIRE(nullptr != result->open(path, mode), path, mode);
  return std::move(result);
}

}  // namespace

struct ColumnFileTest : public testing::Test {
 public:
  std::string TemporaryDirectory() {
    const char* tmpdir = getenv("TMPDIR");
    if (!tmpdir) tmpdir = "/tmp";

    char path[PATH_MAX];
    strcpy(path, tmpdir);
    strcat(path, "/test.XXXXXX");

    KJ_REQUIRE(nullptr != mkdtemp(path), std::strerror(errno));

    return path;
  }
};

TEST_F(ColumnFileTest, WriteTableToFile) {
  const auto tmp_dir = TemporaryDirectory();
  KJ_DEFER(KJ_SYSCALL(rmdir(tmp_dir.c_str())));

  const ColumnFileCompression compression_methods[] = {
      kColumnFileCompressionNone, kColumnFileCompressionSnappy,
      kColumnFileCompressionLZ4,  kColumnFileCompressionLZMA,
      kColumnFileCompressionZLIB,
  };

  for (const auto compression_method : compression_methods) {
    const auto tmp_path =
        kj::str(tmp_dir, "/test00_", static_cast<int>(compression_method));
    ColumnFileWriter writer(OpenFile(
        tmp_path.cStr(),
        std::ios_base::binary | std::ios_base::out | std::ios_base::trunc));
    KJ_DEFER(KJ_SYSCALL(unlink(tmp_path.cStr())));

    writer.SetCompression(compression_method);

    writer.Put(0, "2000-01-01");
    writer.Put(1, "January");
    writer.Put(2, "First");

    writer.Put(0, "2000-01-02");
    writer.Put(1, "January");
    writer.Put(2, "Second");

    writer.Put(0, "2000-02-02");
    writer.Put(1, "February");
    writer.Put(2, "Second");
    writer.Flush();

    writer.Put(0, "2000-02-03");
    writer.Put(1, "February");
    writer.Put(2, "Third");

    writer.Put(0, "2000-02-03");
    writer.PutNull(1);
    writer.PutNull(2);

    writer.Finalize();

    ColumnFileReader reader(
        OpenFile(tmp_path.cStr(), std::ios_base::binary | std::ios_base::in));

    ASSERT_FALSE(reader.End());

    auto row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-01-01", row[0].second.value());
    EXPECT_EQ("January", row[1].second.value());
    EXPECT_EQ("First", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-01-02", row[0].second.value());
    EXPECT_EQ("January", row[1].second.value());
    EXPECT_EQ("Second", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-02", row[0].second.value());
    EXPECT_EQ("February", row[1].second.value());
    EXPECT_EQ("Second", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-03", row[0].second.value());
    EXPECT_EQ("February", row[1].second.value());
    EXPECT_EQ("Third", row[2].second.value());

    EXPECT_FALSE(reader.End());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-03", row[0].second.value());
    EXPECT_FALSE(row[1].second);
    EXPECT_FALSE(row[2].second);

    EXPECT_TRUE(reader.End());
  }
}

TEST_F(ColumnFileTest, WriteTableToString) {
  const ColumnFileCompression compression_methods[] = {
      kColumnFileCompressionNone, kColumnFileCompressionSnappy,
      kColumnFileCompressionLZ4,  kColumnFileCompressionLZMA,
      kColumnFileCompressionZLIB,
  };

  for (const auto compression_method : compression_methods) {
    std::string buffer;

    ColumnFileWriter writer(buffer);

    writer.SetCompression(compression_method);

    writer.Put(0, "2000-01-01");
    writer.Put(1, "January");
    writer.Put(2, "First");

    writer.Put(0, "2000-01-02");
    writer.Put(1, "January");
    writer.Put(2, "Second");
    writer.Flush();

    writer.Put(0, "2000-02-02");
    writer.Put(1, "February");
    writer.Put(2, "Second");

    std::string long_string(0xfff, 'x');
    writer.Put(0, "2000-02-03");
    writer.Put(1, "February");
    writer.Put(2, long_string);

    writer.Put(0, "2000-02-03");
    writer.PutNull(1);
    writer.PutNull(2);
    writer.Finalize();

    ColumnFileReader reader(buffer);

    EXPECT_FALSE(reader.End());

    auto row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-01-01", row[0].second.value());
    EXPECT_EQ("January", row[1].second.value());
    EXPECT_EQ("First", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-01-02", row[0].second.value());
    EXPECT_EQ("January", row[1].second.value());
    EXPECT_EQ("Second", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-02", row[0].second.value());
    EXPECT_EQ("February", row[1].second.value());
    EXPECT_EQ("Second", row[2].second.value());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-03", row[0].second.value());
    EXPECT_EQ("February", row[1].second.value());
    EXPECT_EQ(long_string, row[2].second.value());

    EXPECT_FALSE(reader.End());

    row = reader.GetRow();
    EXPECT_EQ(3U, row.size());
    EXPECT_EQ("2000-02-03", row[0].second.value());
    EXPECT_FALSE(row[1].second);
    EXPECT_FALSE(row[2].second);

    EXPECT_TRUE(reader.End());
  }
}

TEST_F(ColumnFileTest, WriteMessageToString) {
  capnp::SchemaParser schema_parser;
  kj::ArrayPtr<const kj::StringPtr> import_path;
  auto parsed_schema = schema_parser.parseDiskFile(
      "testdata/addressbook.capnp", "testdata/addressbook.capnp", import_path);
  auto address_book_schema = parsed_schema.getNested("AddressBook");

  kj::Array<capnp::word> words;

  capnp::MallocMessageBuilder orig_message;
  auto orig_address_book = orig_message.initRoot<capnp::DynamicStruct>(
      address_book_schema.asStruct());

  {
    auto people = orig_address_book.init("people", 2).as<capnp::DynamicList>();

    auto alice = people[0].as<capnp::DynamicStruct>();
    alice.set("id", 123);
    alice.set("name", "Alice");
    alice.set("email", "alice@example.com");
    auto alice_phones = alice.init("phones", 1).as<capnp::DynamicList>();
    auto phone0 = alice_phones[0].as<capnp::DynamicStruct>();
    phone0.set("number", "555-1212");
    phone0.set("type", "mobile");
#if 0
    // Requires support for unions
    alice.get("employment").as<capnp::DynamicStruct>().set("school", "MIT");
#endif

    auto bob = people[1].as<capnp::DynamicStruct>();
    bob.set("id", 456);
    bob.set("name", "Bob");
    bob.set("email", "bob@example.com");

    words = capnp::messageToFlatArray(orig_message);
  }

  std::string buffer;

  {
    ColumnFileWriter writer(buffer);

    capnp::FlatArrayMessageReader message_reader(words);
    WriteMessageToColumnFile(writer,
                             message_reader.getRoot<capnp::DynamicStruct>(
                                 address_book_schema.asStruct()));
    writer.Finalize();
  }

  {
    ColumnFileReader reader(buffer);

    capnp::MallocMessageBuilder message;
    auto address_book =
        message.initRoot<capnp::DynamicStruct>(address_book_schema.asStruct());

    ReadMessageFromColumnFile(reader, address_book);

#if CAPNP_VERSION >= 6000
    EXPECT_TRUE(address_book.asReader().as<capnp::AnyStruct>() ==
                orig_address_book.asReader().as<capnp::AnyStruct>());
#endif
  }
}

TEST_F(ColumnFileTest, AFLTestCases) {
  std::vector<std::string> test_cases;

  auto dir = opendir("testdata");
  if (!dir) {
    KJ_FAIL_SYSCALL("opendir", errno);
  }
  KJ_DEFER(KJ_SYSCALL(closedir(dir)));

  while (auto ent = readdir(dir)) {
    if (!HasSuffix(ent->d_name, ".col")) continue;

    auto path = kj::str("testdata/", ent->d_name);
    try {
      ColumnFileReader reader(
          OpenFile(path.cStr(), std::ios_base::binary | std::ios_base::in));
      while (!reader.End()) reader.GetRow();
    } catch (std::runtime_error& e) {
      fprintf(stderr, "runtime_error: %s\n", e.what());
    } catch (std::bad_alloc&) {
      fprintf(stderr, "bad_alloc\n");
    } catch (std::out_of_range&) {
      fprintf(stderr, "range error\n");
    }
  }
}

TEST_F(ColumnFileTest, UnsignedIntegerCoding) {
  static const uint32_t kTestNumbers[] = {
      0,          0x10U,      0x7fU,       0x80U,      0x100U,    0x1000U,
      0x3fffU,    0x4000U,    0x10000U,    0x100000U,  0x1fffffU, 0x200000U,
      0x1000000U, 0xfffffffU, 0x10000000U, 0xffffffffU};

  for (auto i : kTestNumbers) {
    std::string buffer;
    cantera::columnfile_internal::PutUInt(buffer, i);

    EXPECT_TRUE((static_cast<uint8_t>(buffer[0]) & 0xc0) != 0xc0);

    std::string_view read_buffer(buffer);
    auto decoded_int = cantera::columnfile_internal::GetUInt(read_buffer);
    EXPECT_EQ(i, decoded_int);
    EXPECT_TRUE(read_buffer.empty());
  }
}

TEST_F(ColumnFileTest, SignedIntegerCoding) {
  static const int32_t kTestNumbers[] = {
      0,         0x10,      0x7f,       0x80,       0x100,    0x1000,
      0x3fff,    0x4000,    0x10000,    0x100000,   0x1fffff, 0x200000,
      0x1000000, 0xfffffff, 0x10000000, -0x7fffffff};

  for (auto i : kTestNumbers) {
    std::string buffer;
    cantera::columnfile_internal::PutInt(buffer, i);

    EXPECT_TRUE((static_cast<uint8_t>(buffer[0]) & 0xc0) != 0xc0);

    std::string_view read_buffer(buffer);
    auto decoded_int = cantera::columnfile_internal::GetInt(read_buffer);
    EXPECT_EQ(i, decoded_int);
    EXPECT_TRUE(read_buffer.empty());
  }
}

TEST_F(ColumnFileTest, Replace) {
  const auto tmp_dir = TemporaryDirectory();
  KJ_DEFER(KJ_SYSCALL(rmdir(tmp_dir.c_str())));

  const auto tmp_path0 = kj::str(tmp_dir, "/test01");
  ColumnFileWriter writer(OpenFile(tmp_path0.cStr(), std::ios_base::binary |
                                                         std::ios_base::out |
                                                         std::ios_base::trunc));
  KJ_DEFER(KJ_SYSCALL(unlink(tmp_path0.cStr())));

  writer.Put(0, "aaa");
  writer.Put(1, "0");

  const auto tmp_path1 = kj::str(tmp_dir, "/test02");
  writer = ColumnFileWriter(OpenFile(
      tmp_path1.cStr(),
      std::ios_base::binary | std::ios_base::out | std::ios_base::trunc));
  KJ_DEFER(KJ_SYSCALL(unlink(tmp_path1.cStr())));

  writer.Put(0, "bbb");
  writer.Put(1, "1");

  writer.Finalize();

  ColumnFileReader reader0(
      OpenFile(tmp_path0.cStr(), std::ios_base::binary | std::ios_base::in));
  ASSERT_FALSE(reader0.End());

  auto row = reader0.GetRow();
  EXPECT_EQ(2U, row.size());
  EXPECT_EQ("aaa", row[0].second.value());
  EXPECT_EQ("0", row[1].second.value());

  ColumnFileReader reader1(
      OpenFile(tmp_path1.cStr(), std::ios_base::binary | std::ios_base::in));
  ASSERT_FALSE(reader1.End());

  row = reader1.GetRow();
  EXPECT_EQ(2U, row.size());
  EXPECT_EQ("bbb", row[0].second.value());
  EXPECT_EQ("1", row[1].second.value());
}
