#include "test_utils.h"

#include <string>
#include <vector>

namespace {

TEST(RowStorageTest, PutGetOverwriteAndReopen) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("row-storage");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 4;
  options.rowShardCount = 4;
  options.maxLoadFactor = 0.5;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::vector<std::byte> a = LumoDB::test::MakeBytes("row-a");
  std::vector<std::byte> b = LumoDB::test::MakeBytes("row-b");
  std::vector<std::byte> bNew = LumoDB::test::MakeBytes("row-b-new");
  std::vector<LumoDB::RowStructEntry> entries = {
      {.key = "a", .flatBufferBytes = a},
      {.key = "b", .flatBufferBytes = b},
      {.key = "b", .flatBufferBytes = bNew},
  };

  ASSERT_OK(database.PutRowStructs("StructA", 7, entries));
  EXPECT_EQ(database.RowCount(), 1);

  std::vector<std::byte> value;
  ASSERT_OK(database.GetRowStruct("StructA", 7, "a", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "row-a");
  ASSERT_OK(database.GetRowStruct("StructA", 7, "b", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "row-b-new");
  EXPECT_EQ(database.GetRowStruct("StructA", 7, "missing", value).Code(),
            LumoDB::StatusCode::kNotFound);
  EXPECT_EQ(database.GetRowStruct("StructA", 8, "a", value).Code(),
            LumoDB::StatusCode::kNotFound);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.GetRowStruct("StructA", 7, "b", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "row-b-new");
  ASSERT_OK(database.Close());
}

TEST(RowStorageTest, RejectsEmptyRow) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("row-storage-empty");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));
  EXPECT_EQ(database.PutRowStructs("StructA", 1, {}).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Close());
}

TEST(RowStorageTest, PutRowStructAddsKeysSeriallyWithinRow) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("row-storage-single-key");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 16;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  ASSERT_OK(
      database.PutRowStruct("StructA", 42, "first", LumoDB::test::MakeBytes("one")));
  ASSERT_OK(
      database.PutRowStruct("StructA", 42, "second", LumoDB::test::MakeBytes("two")));
  ASSERT_OK(database.PutRowStruct("StructA", 42, "first",
                                  LumoDB::test::MakeBytes("one-updated")));

  EXPECT_EQ(database.RowCount(), 1);

  std::vector<std::byte> value;
  ASSERT_OK(database.GetRowStruct("StructA", 42, "first", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "one-updated");
  ASSERT_OK(database.GetRowStruct("StructA", 42, "second", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "two");

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.GetRowStruct("StructA", 42, "first", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "one-updated");
  ASSERT_OK(database.GetRowStruct("StructA", 42, "second", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "two");
  ASSERT_OK(database.Close());
}

TEST(RowStorageTest, ReadsSingleValueFromLargeRow) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("row-storage-sparse-read");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));

  const std::vector<std::byte> target = LumoDB::test::MakeBytes("target-value");
  const std::vector<std::byte> largeValue(1024 * 1024, std::byte{0x5a});
  std::vector<LumoDB::RowStructEntry> entries = {
      {.key = "target", .flatBufferBytes = target},
  };
  std::vector<std::string> largeKeys;
  largeKeys.reserve(8);
  for (uint64_t index = 0; index < 8; ++index) {
    largeKeys.push_back("large-" + std::to_string(index));
    entries.push_back({.key = largeKeys.back(), .flatBufferBytes = largeValue});
  }
  ASSERT_OK(database.PutRowStructs("StructA", 99, entries));

  std::vector<std::byte> value;
  ASSERT_OK(database.GetRowStruct("StructA", 99, "target", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "target-value");
  ASSERT_OK(database.Close());
}

}  // namespace
