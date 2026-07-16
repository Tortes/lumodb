#include "test_utils.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <limits>
#include <vector>

#include "lumodb/file.h"
#include "lumodb/storage_format.h"

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
    values.emplace_back(8192, static_cast<std::byte>(index & 0xff));
    entries.push_back({.key = keys.back(), .flatBufferBytes = values.back()});
  }
  ASSERT_OK(database.PutStructs("StructA", entries));
  EXPECT_EQ(database.EntryCount(), kEntryCount);

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "key-0", value));
  EXPECT_EQ(value, values[0]);
  ASSERT_OK(database.GetStruct("StructA", "key-299", value));
  EXPECT_EQ(value, values[299]);
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutUniqueStructsWritesNewKeysWithoutUpsertLookup) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-batch");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));

  const std::vector<std::byte> first = LumoDB::test::MakeBytes("first");
  const std::vector<std::byte> second = LumoDB::test::MakeBytes("second");
  const std::vector<LumoDB::StructEntry> entries = {
      {.key = "first", .flatBufferBytes = first},
      {.key = "second", .flatBufferBytes = second},
  };
  ASSERT_OK(database.PutUniqueStructs("StructA", entries));

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "first", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "first");
  ASSERT_OK(database.GetStruct("StructA", "second", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "second");
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutUniqueStructsStreamsOneLargeMixedBatch) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-one-large-batch");

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));

  constexpr uint64_t kEntryCount = 200000;
  const std::vector<std::byte> smallPayload(32, std::byte{0x31});
  const std::vector<std::byte> largePayload(8192, std::byte{0x72});
  std::vector<std::string> keys;
  std::vector<LumoDB::StructEntry> entries;
  keys.reserve(kEntryCount);
  entries.reserve(kEntryCount);

  uint64_t expectedValueFileSize = sizeof(LumoDB::detail::ValueFileHeader);
  for (uint64_t index = 0; index < kEntryCount; ++index) {
    keys.push_back("stream-key-" + std::to_string(index));
    const std::vector<std::byte>& payload =
        index < 300 || index % 997 == 0 ? largePayload : smallPayload;
    entries.push_back({.key = keys.back(), .flatBufferBytes = payload});
    expectedValueFileSize += sizeof(LumoDB::detail::ValueRecordHeader) +
                             std::string_view("StructA").size() +
                             keys.back().size() + payload.size();
  }

  ASSERT_OK(database.PutUniqueStructs("StructA", entries));
  EXPECT_EQ(database.EntryCount(), kEntryCount);
  EXPECT_EQ(database.BucketCount(), 262144);
  EXPECT_EQ(std::filesystem::file_size(directory / "values.lumov"),
            expectedValueFileSize);

  std::vector<std::byte> value;
  for (const uint64_t index : {0ULL, 299ULL, 300ULL, 997ULL,
                               kEntryCount / 2, kEntryCount - 1}) {
    ASSERT_OK(database.GetStruct("StructA", keys[index], value));
    const std::vector<std::byte>& expected =
        index < 300 || index % 997 == 0 ? largePayload : smallPayload;
    EXPECT_EQ(value, expected);
  }

  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory));
  ASSERT_OK(database.GetStruct("StructA", keys.back(), value));
  EXPECT_EQ(value, smallPayload);
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutUniqueStructsFillsASubWordIndex) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-small-index");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 16;
  options.maxLoadFactor = 0.94;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kEntryCount = 15;
  const std::vector<std::byte> payload = LumoDB::test::MakeBytes("value");
  std::vector<std::string> keys;
  std::vector<LumoDB::StructEntry> entries;
  keys.reserve(kEntryCount);
  entries.reserve(kEntryCount);
  for (uint64_t index = 0; index < kEntryCount; ++index) {
    keys.push_back("small-key-" + std::to_string(index));
    entries.push_back({.key = keys.back(), .flatBufferBytes = payload});
  }

  ASSERT_OK(database.PutUniqueStructs("StructA", entries));
  EXPECT_EQ(database.EntryCount(), kEntryCount);
  EXPECT_EQ(database.BucketCount(), 16);
  for (const std::string& key : keys) {
    std::vector<std::byte> value;
    ASSERT_OK(database.GetStruct("StructA", key, value));
    EXPECT_EQ(value, payload);
  }
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutUniqueStructsHandlesPackedChunksAndDenseIndexes) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-packed-dense");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 32768;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kInitialEntryCount = 26000;
  const std::vector<std::byte> payload(256, std::byte{0x5a});
  std::vector<std::string> keys;
  std::vector<LumoDB::StructEntry> entries;
  keys.reserve(kInitialEntryCount);
  entries.reserve(kInitialEntryCount);
  for (uint64_t index = 0; index < kInitialEntryCount; ++index) {
    keys.push_back("packed-key-" + std::to_string(index));
    entries.push_back({.key = keys.back(), .flatBufferBytes = payload});
  }
  ASSERT_OK(database.PutUniqueStructs("StructA", entries));
  EXPECT_EQ(database.EntryCount(), kInitialEntryCount);
  EXPECT_EQ(database.BucketCount(), 32768);

  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", keys.front(), value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.GetStruct("StructA", keys.back(), value));
  EXPECT_EQ(value, payload);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));

  const std::vector<std::byte> mutableValue =
      LumoDB::test::MakeBytes("mutable-value");
  ASSERT_OK(database.PutStruct("StructA", "mutable-key", mutableValue));

  std::vector<std::string> reopenedKeys;
  std::vector<LumoDB::StructEntry> reopenedEntries;
  reopenedKeys.reserve(100);
  reopenedEntries.reserve(100);
  for (uint64_t index = kInitialEntryCount;
       index < kInitialEntryCount + 100; ++index) {
    reopenedKeys.push_back("packed-key-" + std::to_string(index));
    reopenedEntries.push_back(
        {.key = reopenedKeys.back(), .flatBufferBytes = payload});
  }
  ASSERT_OK(database.PutUniqueStructs("StructA", reopenedEntries));
  EXPECT_EQ(database.BucketCount(), 32768);

  std::vector<std::string> resizeKeys;
  std::vector<LumoDB::StructEntry> resizeEntries;
  resizeKeys.reserve(200);
  resizeEntries.reserve(200);
  for (uint64_t index = kInitialEntryCount + 100;
       index < kInitialEntryCount + 300; ++index) {
    resizeKeys.push_back("packed-key-" + std::to_string(index));
    resizeEntries.push_back({.key = resizeKeys.back(), .flatBufferBytes = payload});
  }
  ASSERT_OK(database.PutUniqueStructs("StructA", resizeEntries));
  EXPECT_EQ(database.EntryCount(), kInitialEntryCount + 301);
  EXPECT_EQ(database.BucketCount(), 65536);

  ASSERT_OK(database.GetStruct("StructA", reopenedKeys.back(), value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.GetStruct("StructA", resizeKeys.back(), value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.GetStruct("StructA", "mutable-key", value));
  EXPECT_EQ(value, mutableValue);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory, options));
  ASSERT_OK(database.GetStruct("StructA", keys.front(), value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.GetStruct("StructA", resizeKeys.back(), value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, PutUniqueStructsReportsCoarseWriteProgress) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-progress");

  std::vector<LumoDB::WriteProgress> progress;
  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 16;
  options.writeProgress = [&](const LumoDB::WriteProgress& update) {
    progress.push_back(update);
  };

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  const std::vector<std::byte> payload(64, std::byte{0x41});
  const std::vector<LumoDB::StructEntry> entries = {
      {.key = "first", .flatBufferBytes = payload},
      {.key = "second", .flatBufferBytes = payload},
  };
  ASSERT_OK(database.PutUniqueStructs("StructA", entries));
  ASSERT_OK(database.Flush());

  const auto hasCompletedPhase = [&](LumoDB::WritePhase phase,
                                     uint64_t total) {
    return std::any_of(progress.begin(), progress.end(),
                       [&](const LumoDB::WriteProgress& update) {
                         return update.phase == phase &&
                                update.completed == total &&
                                update.total == total;
                       });
  };
  EXPECT_TRUE(hasCompletedPhase(LumoDB::WritePhase::kValidating,
                                entries.size()));
  EXPECT_TRUE(hasCompletedPhase(LumoDB::WritePhase::kWritingValues,
                                entries.size()));
  EXPECT_TRUE(hasCompletedPhase(LumoDB::WritePhase::kPublishingIndex,
                                entries.size()));
  EXPECT_TRUE(std::any_of(progress.begin(), progress.end(),
                          [](const LumoDB::WriteProgress& update) {
                            return update.phase == LumoDB::WritePhase::kFlushing &&
                                   update.completed == update.total;
                          }));
  EXPECT_FALSE(std::filesystem::exists(directory / "object_write.lumotxn"));
  ASSERT_OK(database.Close());
}

TEST(StructStorageTest, RecoversAnInterruptedUniqueBatchBeforeOpeningIndex) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-unique-interrupted");

  const std::vector<std::byte> payload = LumoDB::test::MakeBytes("committed");
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory));
  const std::array<LumoDB::StructEntry, 1> committedEntry = {
      LumoDB::StructEntry{.key = "committed-key", .flatBufferBytes = payload},
  };
  ASSERT_OK(database.PutUniqueStructs("StructA", committedEntry));
  ASSERT_OK(database.Close());

  const std::filesystem::path valuePath = directory / "values.lumov";
  const uint64_t rollbackOffset = std::filesystem::file_size(valuePath);
  LumoDB::detail::FileDescriptor valueFile;
  ASSERT_OK(LumoDB::detail::OpenReadWriteCreate(valuePath, valueFile));
  const std::array<std::byte, 17> incompleteTail = {};
  ASSERT_OK(LumoDB::detail::WriteAllAt(valueFile.Get(), incompleteTail.data(),
                                      incompleteTail.size(), rollbackOffset));
  ASSERT_OK(LumoDB::detail::SyncFile(valueFile.Get()));
  valueFile.Reset();

  LumoDB::detail::ObjectWriteMarker marker;
  marker.rollbackOffset = rollbackOffset;
  LumoDB::detail::FileDescriptor markerFile;
  ASSERT_OK(LumoDB::detail::OpenReadWriteCreate(
      directory / "object_write.lumotxn", markerFile));
  ASSERT_OK(LumoDB::detail::TruncateFile(markerFile.Get(), 0));
  ASSERT_OK(LumoDB::detail::WriteAllAt(markerFile.Get(), &marker, sizeof(marker), 0));
  ASSERT_OK(LumoDB::detail::SyncFile(markerFile.Get()));
  markerFile.Reset();
  ASSERT_OK(LumoDB::detail::SyncDirectory(directory));

  EXPECT_EQ(database.OpenReadOnly(directory).Code(),
            LumoDB::StatusCode::kCorruption);
  ASSERT_OK(database.Open(directory));
  EXPECT_EQ(std::filesystem::file_size(valuePath), rollbackOffset);
  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("StructA", "committed-key", value));
  EXPECT_EQ(value, payload);
  ASSERT_OK(database.Close());
  EXPECT_FALSE(std::filesystem::exists(directory / "object_write.lumotxn"));
}

TEST(StructStorageTest, RejectsInitialBucketCountsThatWouldOverflow) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-bucket-overflow");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = std::numeric_limits<uint64_t>::max();
  LumoDB::Database database;
  EXPECT_EQ(database.Open(directory, options).Code(),
            LumoDB::StatusCode::kInvalidArgument);
}

TEST(StructStorageTest, UsesCompactIndexAndMapsClosedDatabaseReadOnly) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("struct-read-only");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 16;
  options.initialRowBucketCount = 16;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  const std::vector<std::byte> objectValue = LumoDB::test::MakeBytes("object-value");
  const std::vector<std::byte> rowValue = LumoDB::test::MakeBytes("row-value");
  const std::vector<LumoDB::RowStructEntry> rowEntries = {
      {.key = "row-key", .flatBufferBytes = rowValue},
  };
  ASSERT_OK(database.PutStruct("ObjectColumn", "object-key", objectValue));
  ASSERT_OK(database.PutRowStructs("RowColumn", 17, rowEntries));

  const uint64_t bucketCount = database.BucketCount();
  ASSERT_OK(database.Close());

  EXPECT_EQ(std::filesystem::file_size(directory / "index.lumoi"),
            sizeof(LumoDB::detail::IndexFileHeader) +
                bucketCount * sizeof(LumoDB::detail::IndexBucket));

  ASSERT_OK(database.OpenReadOnly(directory, options));
  std::vector<std::byte> value;
  ASSERT_OK(database.GetStruct("ObjectColumn", "object-key", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "object-value");
  ASSERT_OK(database.GetRowStruct("RowColumn", 17, "row-key", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "row-value");
  EXPECT_EQ(database.PutStruct("ObjectColumn", "new-key", objectValue).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.PutRowStructs("RowColumn", 18, rowEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Close());
}

}  // namespace
