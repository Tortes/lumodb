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

}  // namespace
