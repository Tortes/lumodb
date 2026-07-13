#include "test_utils.h"

#include <filesystem>
#include <fstream>
#include <vector>

#include "lumodb/storage_format.h"

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

TEST(RebuildTest, MigratesLegacyValueRecordsAndRebuildsTheIndex) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("legacy-value-migration");

  const std::string column = "StructA";
  const std::string key = "key";
  const std::string payload = "payload";
  std::ofstream legacyFile(directory / "values.lumov", std::ios::binary);
  ASSERT_TRUE(legacyFile.is_open());

  LumoDB::detail::ValueFileHeader fileHeader;
  fileHeader.magic = LumoDB::detail::kLegacyValueFileMagic;
  legacyFile.write(reinterpret_cast<const char*>(&fileHeader), sizeof(fileHeader));

  LumoDB::detail::LegacyValueRecordHeader recordHeader;
  recordHeader.sequence = 1;
  recordHeader.columnSize = static_cast<uint32_t>(column.size());
  recordHeader.keySize = static_cast<uint32_t>(key.size());
  recordHeader.valueSize = payload.size();
  legacyFile.write(reinterpret_cast<const char*>(&recordHeader), sizeof(recordHeader));
  legacyFile.write(column.data(), static_cast<std::streamsize>(column.size()));
  legacyFile.write(key.data(), static_cast<std::streamsize>(key.size()));
  legacyFile.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  legacyFile.close();

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));
  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct(column, key, value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), payload);
  ASSERT_OK(database.Close());

  std::ifstream migratedFile(directory / "values.lumov", std::ios::binary);
  ASSERT_TRUE(migratedFile.is_open());
  LumoDB::detail::ValueFileHeader migratedHeader;
  migratedFile.read(reinterpret_cast<char*>(&migratedHeader), sizeof(migratedHeader));
  EXPECT_EQ(migratedHeader.magic, LumoDB::detail::kValueFileMagic);
}

}  // namespace
