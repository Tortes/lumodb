#include "test_utils.h"

#include <vector>

namespace {

TEST(StructStorageTest, PutGetManyAndColumnIsolation) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("struct");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 4;
  options.maxLoadFactor = 0.5;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  ASSERT_OK(database.PutStruct("StructA", "a-1", LumoDB::test::MakeBytes("flatbuffer-a1")));
  ASSERT_OK(database.PutStruct("StructA", "a-2", LumoDB::test::MakeBytes("flatbuffer-a2")));
  ASSERT_OK(database.PutStruct("StructB", "a-1", LumoDB::test::MakeBytes("flatbuffer-b1")));
  ASSERT_OK(
      database.PutStruct("StructA", "a-1", LumoDB::test::MakeBytes("flatbuffer-a1-new")));

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "a-1", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "flatbuffer-a1-new");
  ASSERT_OK(database.GetStruct("StructB", "a-1", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "flatbuffer-b1");

  std::vector<std::vector<std::byte>> values;
  ASSERT_OK(database.GetMany("StructA", {"a-1", "a-2"}, values));
  ASSERT_EQ(values.size(), 2);
  EXPECT_EQ(LumoDB::test::BytesToString(values[0]), "flatbuffer-a1-new");
  EXPECT_EQ(LumoDB::test::BytesToString(values[1]), "flatbuffer-a2");
  EXPECT_EQ(database.EntryCount(), 3);
  EXPECT_GE(database.BucketCount(), 8);
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutStructsWritesBatchesAndUsesTheLastDuplicate) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("struct-batch");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));

  const std::vector<std::byte> first = LumoDB::test::MakeBytes("first");
  const std::vector<std::byte> second = LumoDB::test::MakeBytes("second");
  const std::vector<std::byte> updated = LumoDB::test::MakeBytes("updated");
  const std::vector<LumoDB::StructEntry> entries = {
      {.key = "first", .flatBufferBytes = first},
      {.key = "second", .flatBufferBytes = second},
      {.key = "first", .flatBufferBytes = updated},
  };
  ASSERT_OK(database.PutStructs("StructA", entries));

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "first", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "updated");
  ASSERT_OK(database.GetStruct("StructA", "second", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "second");
  EXPECT_EQ(database.EntryCount(), 2);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory));
  ASSERT_OK(database.GetStruct("StructA", "first", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "updated");
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutStructsHandlesMoreThanOneWritevBatch) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-large-batch");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 1024;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::vector<std::string> keys;
  std::vector<std::vector<std::byte>> values;
  std::vector<LumoDB::StructEntry> entries;
  constexpr uint64_t kEntryCount = 300;
  keys.reserve(kEntryCount);
  values.reserve(kEntryCount);
  entries.reserve(kEntryCount);
  for (uint64_t index = 0; index < kEntryCount; ++index) {
    keys.push_back("key-" + std::to_string(index));
    values.push_back(LumoDB::test::MakeBytes("value-" + std::to_string(index)));
    entries.push_back({.key = keys.back(), .flatBufferBytes = values.back()});
  }
  ASSERT_OK(database.PutStructs("StructA", entries));
  EXPECT_EQ(database.EntryCount(), kEntryCount);

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "key-0", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "value-0");
  ASSERT_OK(database.GetStruct("StructA", "key-299", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "value-299");
  ASSERT_OK(database.Close());
}

}  // namespace
