#include "test_utils.h"

#include <filesystem>
#include <vector>

namespace {

TEST(RebuildTest, RebuildsObjectIndexFromValueFile) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("rebuild");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));
  ASSERT_OK(database.PutStruct("StructA", "key", LumoDB::test::MakeBytes("payload")));
  ASSERT_OK(database.Close());

  std::filesystem::remove(directory / "index.lumoi");

  ASSERT_OK(database.Open(directory));
  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "key", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "payload");
  EXPECT_EQ(database.EntryCount(), 1);
  ASSERT_OK(database.Close());
}

TEST(RebuildTest, RebuildsRowIndexFromShardFiles) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("row-rebuild");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 4;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::vector<std::byte> payload = LumoDB::test::MakeBytes("payload");
  std::vector<LumoDB::RowStructEntry> entries = {
      {.key = "key", .flatBufferBytes = payload},
  };
  ASSERT_OK(database.PutRowStructs("StructA", 9, entries));
  ASSERT_OK(database.Close());

  std::filesystem::remove(directory / "row_index.lumori");

  ASSERT_OK(database.Open(directory, options));
  std::vector<std::byte> value;
  ASSERT_OK(database.GetRowStruct("StructA", 9, "key", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "payload");
  EXPECT_EQ(database.RowCount(), 1);
  ASSERT_OK(database.Close());
}

}  // namespace
