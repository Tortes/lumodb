#include <array>
#include <atomic>
#include <bit>
#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.h"

#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "lumodb/storage_format.h"

namespace {

LumoDB::DatabaseOptions TestOptions(uint64_t expectedEntries = 10'000) {
  LumoDB::DatabaseOptions options;
  options.expectedEntryCountPerColumn = expectedEntries;
  options.expectedAutomaticColumnCount = 2;
  options.expectedExplicitRowCount = 64;
  options.expectedExplicitEntryCount = expectedEntries;
  options.averageKeyBytes = 16;
  options.averageValueBytes = 32;
  options.maxEntriesPerRow = 768;
  options.writerThreadCount = 4;
  options.rowShardCount = 4;
  options.memoryBudgetBytes = 64ULL * 1024 * 1024;
  options.stageBufferBytes = 16 * 1024;
  return options;
}

TEST(DatabaseTest, NewDatabaseRequiresExpectedScale) {
  const auto directory = LumoDB::test::MakeTestDirectory("requires-scale");
  LumoDB::Database database;
  const LumoDB::Status status = database.Open(directory);
  EXPECT_EQ(status.Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_NE(status.Message().find("expectedEntryCountPerColumn"), std::string::npos);
}

TEST(DatabaseTest, PutFlushGetAndPersistedAutomaticLayout) {
  const auto directory = LumoDB::test::MakeTestDirectory("automatic-layout");
  const LumoDB::DatabaseOptions options = TestOptions();
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  const LumoDB::DatabaseLayout layout = database.Layout();
  EXPECT_EQ(layout.routeCountPerColumn, 16);
  EXPECT_EQ(layout.expectedExplicitRowCount, 64);
  EXPECT_EQ(layout.targetEntriesPerRow, 768);
  EXPECT_EQ(layout.rowShardCount, 4);
  EXPECT_TRUE(std::has_single_bit(layout.rowIndexBucketCount));

  ASSERT_OK(database.Put("users", "alice", "one"));
  ASSERT_OK(database.Put("users", "bob", "two"));
  ASSERT_OK(database.Put("orders", "alice", "other-column"));
  EXPECT_TRUE(database.HasUncommittedWrites());

  std::string value;
  EXPECT_EQ(database.Get("users", "alice", value).Code(), LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Flush());
  EXPECT_FALSE(database.HasUncommittedWrites());
  ASSERT_OK(database.Get("users", "alice", value));
  EXPECT_EQ(value, "one");
  ASSERT_OK(database.Get("users", "bob", value));
  EXPECT_EQ(value, "two");
  ASSERT_OK(database.Get("orders", "alice", value));
  EXPECT_EQ(value, "other-column");
  EXPECT_EQ(database.EntryCount(), 3);
  ASSERT_OK(database.Close());

  ASSERT_OK(database.OpenReadOnly(directory));
  EXPECT_EQ(database.Layout().routeCountPerColumn, layout.routeCountPerColumn);
  ASSERT_OK(database.Get("users", "alice", value));
  EXPECT_EQ(value, "one");
  EXPECT_EQ(database.Put("users", "x", "y").Code(), LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Close());
}

TEST(DatabaseTest, LastDuplicateWinsWithinAndAcrossFlushes) {
  const auto directory = LumoDB::test::MakeTestDirectory("duplicates");
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, TestOptions(1'000)));
  ASSERT_OK(database.Put("column", "key", "first"));
  ASSERT_OK(database.Put("column", "key", "second"));
  ASSERT_OK(database.Put("column", "other", "untouched"));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 2);

  std::string value;
  ASSERT_OK(database.Get("column", "key", value));
  EXPECT_EQ(value, "second");
  ASSERT_OK(database.Put("column", "key", "third"));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 2);
  ASSERT_OK(database.Get("column", "key", value));
  EXPECT_EQ(value, "third");
  ASSERT_OK(database.Get("column", "other", value));
  EXPECT_EQ(value, "untouched");
}

TEST(DatabaseTest, DuplicateHeavyBatchSizesRowFromUniqueKeys) {
  const auto directory = LumoDB::test::MakeTestDirectory("duplicate-heavy");
  constexpr uint32_t kDuplicateCount = 20'000;
  LumoDB::DatabaseOptions options = TestOptions(kDuplicateCount);
  options.writerThreadCount = 1;
  options.rowShardCount = 1;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  for (uint32_t index = 0; index < kDuplicateCount; ++index) {
    ASSERT_OK(database.Put("column", "key", "value-" + std::to_string(index)));
  }
  ASSERT_OK(database.Flush());

  EXPECT_EQ(database.EntryCount(), 1);
  EXPECT_EQ(database.RowCount(), 1);
  std::string value;
  ASSERT_OK(database.Get("column", "key", value));
  EXPECT_EQ(value, "value-19999");
  EXPECT_LT(std::filesystem::file_size(directory / "row_values-000.lumorv"), 64ULL * 1024);
}

TEST(DatabaseTest, PutStructsStagesAutomaticBatchAndPersistsUpdates) {
  const auto directory = LumoDB::test::MakeTestDirectory("struct-batch");
  LumoDB::DatabaseOptions options = TestOptions(10'000);
  options.stageBufferBytes = 4 * 1024;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.PutStructs("", {}));
  EXPECT_FALSE(database.HasUncommittedWrites());

  const std::vector<std::byte> binary = {std::byte{0x00}, std::byte{0x7f}, std::byte{0xff}};
  const std::vector<std::byte> duplicateOld = LumoDB::test::MakeBytes("old");
  const std::vector<std::byte> duplicateNew = LumoDB::test::MakeBytes("new");
  const std::vector<std::byte> largeValue(128 * 1024, std::byte{0x5a});
  const std::vector<std::byte> emptyValue;
  const std::vector<LumoDB::StructEntry> entries = {
      {.key = "binary", .flatBufferBytes = binary},
      {.key = "duplicate", .flatBufferBytes = duplicateOld},
      {.key = "large", .flatBufferBytes = largeValue},
      {.key = "duplicate", .flatBufferBytes = duplicateNew},
      {.key = "empty", .flatBufferBytes = emptyValue},
  };
  ASSERT_OK(database.PutStructs("batch", entries));
  {
    const std::vector<std::byte> transientValue = LumoDB::test::MakeBytes("copied-immediately");
    const std::array<LumoDB::StructEntry, 1> transientEntries = {
        LumoDB::StructEntry{.key = "transient", .flatBufferBytes = transientValue},
    };
    ASSERT_OK(database.PutStructs("batch", transientEntries));
  }
  ASSERT_OK(database.Put("batch", "single", "single-value"));

  std::vector<std::byte> value;
  EXPECT_EQ(database.Get("batch", "binary", value).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 6);
  ASSERT_OK(database.Get("batch", "binary", value));
  EXPECT_EQ(value, binary);
  ASSERT_OK(database.Get("batch", "duplicate", value));
  EXPECT_EQ(value, duplicateNew);
  ASSERT_OK(database.Get("batch", "large", value));
  EXPECT_EQ(value, largeValue);
  ASSERT_OK(database.Get("batch", "empty", value));
  EXPECT_TRUE(value.empty());
  ASSERT_OK(database.Get("batch", "transient", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "copied-immediately");

  const std::vector<std::byte> updated = LumoDB::test::MakeBytes("updated");
  const std::vector<std::byte> added = LumoDB::test::MakeBytes("added");
  const std::array<LumoDB::StructEntry, 2> updates = {
      LumoDB::StructEntry{.key = "duplicate", .flatBufferBytes = updated},
      LumoDB::StructEntry{.key = "added", .flatBufferBytes = added},
  };
  ASSERT_OK(database.PutStructs("batch", updates));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 7);
  ASSERT_OK(database.Get("batch", "duplicate", value));
  EXPECT_EQ(value, updated);

  const std::vector<std::byte> closeFlushed = LumoDB::test::MakeBytes("close-flushed");
  const std::array<LumoDB::StructEntry, 1> closeEntries = {
      LumoDB::StructEntry{.key = "close", .flatBufferBytes = closeFlushed},
  };
  ASSERT_OK(database.PutStructs("batch", closeEntries));
  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory));
  ASSERT_OK(database.Get("batch", "close", value));
  EXPECT_EQ(value, closeFlushed);
  EXPECT_EQ(database.PutStructs("batch", updates).Code(),
            LumoDB::StatusCode::kInvalidArgument);
}

TEST(DatabaseTest, PutStructsPreservesDuplicateOrderAcrossRoutingChunks) {
  const auto directory = LumoDB::test::MakeTestDirectory("struct-batch-chunks");
  constexpr size_t kUniqueEntries = 64 * 1024;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, TestOptions(kUniqueEntries)));

  std::vector<std::string> keys;
  keys.reserve(kUniqueEntries);
  for (size_t index = 0; index < kUniqueEntries; ++index) {
    keys.push_back("key-" + std::to_string(index));
  }
  const std::vector<std::byte> original = LumoDB::test::MakeBytes("original");
  const std::vector<std::byte> updated = LumoDB::test::MakeBytes("updated-after-chunk");
  std::vector<LumoDB::StructEntry> entries;
  entries.reserve(kUniqueEntries + 1);
  for (const std::string& key : keys) {
    entries.push_back({.key = key, .flatBufferBytes = original});
  }
  entries.push_back({.key = keys.front(), .flatBufferBytes = updated});

  ASSERT_OK(database.PutStructs("chunked", entries));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), kUniqueEntries);
  std::vector<std::byte> value;
  ASSERT_OK(database.Get("chunked", keys.front(), value));
  EXPECT_EQ(value, updated);
  ASSERT_OK(database.Get("chunked", keys.back(), value));
  EXPECT_EQ(value, original);
}

TEST(DatabaseTest, PutRowStructsStagesBinaryBatchAndPersistsUpdates) {
  const auto directory = LumoDB::test::MakeTestDirectory("row-struct-batch");
  LumoDB::DatabaseOptions options = TestOptions(10'000);
  options.stageBufferBytes = 4 * 1024;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  const std::vector<std::byte> binary = {std::byte{0x00}, std::byte{0x7f}, std::byte{0xff}};
  const std::vector<std::byte> duplicateOld = LumoDB::test::MakeBytes("old");
  const std::vector<std::byte> duplicateNew = LumoDB::test::MakeBytes("new");
  const std::vector<std::byte> largeValue(128 * 1024, std::byte{0x5a});
  const std::vector<std::byte> emptyValue;
  const std::vector<LumoDB::RowStructEntry> entries = {
      {.key = "binary", .flatBufferBytes = binary},
      {.key = "duplicate", .flatBufferBytes = duplicateOld},
      {.key = "large", .flatBufferBytes = largeValue},
      {.key = "duplicate", .flatBufferBytes = duplicateNew},
      {.key = "empty", .flatBufferBytes = emptyValue},
  };

  ASSERT_OK(database.PutRowStructs("mixed-batch", 7, entries));
  {
    const std::vector<std::byte> transientValue = LumoDB::test::MakeBytes("copied-immediately");
    const std::array<LumoDB::RowStructEntry, 1> transientEntries = {
        LumoDB::RowStructEntry{.key = "transient", .flatBufferBytes = transientValue},
    };
    ASSERT_OK(database.PutRowStructs("mixed-batch", 7, transientEntries));
  }
  ASSERT_OK(database.Put("mixed-batch", 7, "single", "single-explicit"));
  ASSERT_OK(database.Put("mixed-batch", "duplicate", "automatic-value"));
  EXPECT_TRUE(database.HasUncommittedWrites());

  std::vector<std::byte> value;
  EXPECT_EQ(database.Get("mixed-batch", 7, "binary", value).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 7);
  EXPECT_EQ(database.RowCount(), 2);

  ASSERT_OK(database.Get("mixed-batch", 7, "binary", value));
  EXPECT_EQ(value, binary);
  ASSERT_OK(database.Get("mixed-batch", 7, "duplicate", value));
  EXPECT_EQ(value, duplicateNew);
  ASSERT_OK(database.Get("mixed-batch", 7, "large", value));
  EXPECT_EQ(value, largeValue);
  ASSERT_OK(database.Get("mixed-batch", 7, "empty", value));
  EXPECT_TRUE(value.empty());
  ASSERT_OK(database.Get("mixed-batch", 7, "single", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "single-explicit");
  ASSERT_OK(database.Get("mixed-batch", 7, "transient", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "copied-immediately");
  ASSERT_OK(database.Get("mixed-batch", "duplicate", value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), "automatic-value");

  const std::vector<std::byte> updated = LumoDB::test::MakeBytes("updated");
  const std::vector<std::byte> added = LumoDB::test::MakeBytes("added");
  const std::array<LumoDB::RowStructEntry, 2> updates = {
      LumoDB::RowStructEntry{.key = "duplicate", .flatBufferBytes = updated},
      LumoDB::RowStructEntry{.key = "added", .flatBufferBytes = added},
  };
  ASSERT_OK(database.PutRowStructs("mixed-batch", 7, updates));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 8);
  ASSERT_OK(database.Get("mixed-batch", 7, "duplicate", value));
  EXPECT_EQ(value, updated);
  ASSERT_OK(database.Get("mixed-batch", 7, "binary", value));
  EXPECT_EQ(value, binary);

  const std::vector<std::byte> closeFlushed = LumoDB::test::MakeBytes("close-flushed");
  const std::array<LumoDB::RowStructEntry, 1> closeEntries = {
      LumoDB::RowStructEntry{.key = "close", .flatBufferBytes = closeFlushed},
  };
  ASSERT_OK(database.PutRowStructs("mixed-batch", 8, closeEntries));
  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory));
  ASSERT_OK(database.Get("mixed-batch", 7, "added", value));
  EXPECT_EQ(value, added);
  ASSERT_OK(database.Get("mixed-batch", 8, "close", value));
  EXPECT_EQ(value, closeFlushed);
  EXPECT_EQ(database.PutRowStructs("mixed-batch", 7, updates).Code(),
            LumoDB::StatusCode::kInvalidArgument);
}

TEST(DatabaseTest, GetRowStructDirectlySelectsExplicitRow) {
  const auto directory = LumoDB::test::MakeTestDirectory("get-row-struct");
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, TestOptions(1'000)));

  const std::vector<std::byte> rowSeven = LumoDB::test::MakeBytes("row-seven");
  const std::vector<std::byte> rowEight = {std::byte{0x00}, std::byte{0xff}};
  const std::array<LumoDB::RowStructEntry, 1> entriesSeven = {
      LumoDB::RowStructEntry{.key = "same-key", .flatBufferBytes = rowSeven},
  };
  const std::array<LumoDB::RowStructEntry, 1> entriesEight = {
      LumoDB::RowStructEntry{.key = "same-key", .flatBufferBytes = rowEight},
  };
  ASSERT_OK(database.PutRowStructs("rows", 7, entriesSeven));
  ASSERT_OK(database.PutRowStructs("rows", 8, entriesEight));
  ASSERT_OK(database.Put("rows", "same-key", "automatic"));

  std::vector<std::byte> value = LumoDB::test::MakeBytes("stale");
  EXPECT_EQ(database.GetRowStruct("rows", 7, "same-key", value).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_TRUE(value.empty());
  ASSERT_OK(database.Flush());

  ASSERT_OK(database.GetRowStruct("rows", 7, "same-key", value));
  EXPECT_EQ(value, rowSeven);
  ASSERT_OK(database.GetRowStruct("rows", 8, "same-key", value));
  EXPECT_EQ(value, rowEight);
  EXPECT_EQ(database.GetRowStruct("rows", 9, "same-key", value).Code(),
            LumoDB::StatusCode::kNotFound);
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(database.GetRowStruct("rows", 1ULL << 63, "same-key", value).Code(),
            LumoDB::StatusCode::kInvalidArgument);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory));
  ASSERT_OK(database.GetRowStruct("rows", 7, "same-key", value));
  EXPECT_EQ(value, rowSeven);
}

TEST(DatabaseTest, AutomaticAndExplicitRowsCanCoexist) {
  const auto directory = LumoDB::test::MakeTestDirectory("mixed-row-routing");
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, TestOptions(1'000)));

  // Different columns may choose different routing modes.
  ASSERT_OK(database.Put("automatic", "same-key", "auto-value"));
  ASSERT_OK(database.Put("explicit", 7, "same-key", "row-seven"));
  ASSERT_OK(database.Put("explicit", 8, "same-key", "row-eight"));

  // Both modes may also share one column/key without colliding.
  ASSERT_OK(database.Put("mixed", "same-key", "mixed-auto"));
  ASSERT_OK(database.Put("mixed", 0, "same-key", "mixed-explicit"));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 5);

  std::string value;
  ASSERT_OK(database.Get("automatic", "same-key", value));
  EXPECT_EQ(value, "auto-value");
  ASSERT_OK(database.Get("explicit", 7, "same-key", value));
  EXPECT_EQ(value, "row-seven");
  ASSERT_OK(database.Get("explicit", 8, "same-key", value));
  EXPECT_EQ(value, "row-eight");
  ASSERT_OK(database.Get("mixed", "same-key", value));
  EXPECT_EQ(value, "mixed-auto");
  ASSERT_OK(database.Get("mixed", 0, "same-key", value));
  EXPECT_EQ(value, "mixed-explicit");
  EXPECT_EQ(database.Get("explicit", 9, "same-key", value).Code(), LumoDB::StatusCode::kNotFound);

  ASSERT_OK(database.Put("explicit", 7, "same-key", "row-seven-updated"));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 5);
  ASSERT_OK(database.Get("explicit", 7, "same-key", value));
  EXPECT_EQ(value, "row-seven-updated");

  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory));
  ASSERT_OK(database.Get("mixed", 0, "same-key", value));
  EXPECT_EQ(value, "mixed-explicit");
}

TEST(DatabaseTest, PutIsThreadSafe) {
  const auto directory = LumoDB::test::MakeTestDirectory("parallel-put");
  constexpr uint32_t kThreads = 8;
  constexpr uint32_t kEntryCount = 40'000;
  LumoDB::DatabaseOptions options = TestOptions(kEntryCount);
  options.writerThreadCount = kThreads;
  options.rowShardCount = kThreads;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    threads.emplace_back([&, threadId] {
      for (uint32_t index = threadId; index < kEntryCount; index += kThreads) {
        const std::string key = "key-" + std::to_string(index);
        const std::string value = "value-" + std::to_string(index);
        const LumoDB::Status status =
            index % 2 == 0 ? database.Put("automatic-parallel", key, value)
                           : database.Put("explicit-parallel", index % 64, key, value);
        if (!status) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  ASSERT_FALSE(failed.load(std::memory_order_relaxed));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), kEntryCount);

  for (uint32_t index : {0U, 1U, 19'999U, 39'999U}) {
    std::string value;
    if (index % 2 == 0) {
      ASSERT_OK(database.Get("automatic-parallel", "key-" + std::to_string(index), value));
    } else {
      ASSERT_OK(
          database.Get("explicit-parallel", index % 64, "key-" + std::to_string(index), value));
    }
    EXPECT_EQ(value, "value-" + std::to_string(index));
  }
}

TEST(DatabaseTest, PutStructsIsThreadSafeAcrossConcurrentBatches) {
  const auto directory = LumoDB::test::MakeTestDirectory("parallel-struct-batches");
  constexpr uint32_t kThreads = 8;
  constexpr uint32_t kBatchesPerThread = 100;
  constexpr uint32_t kEntriesPerBatch = 4;
  constexpr uint32_t kEntryCount = kThreads * kBatchesPerThread * kEntriesPerBatch;
  LumoDB::DatabaseOptions options = TestOptions(kEntryCount);
  options.writerThreadCount = kThreads;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    threads.emplace_back([&, threadId] {
      for (uint32_t batch = 0; batch < kBatchesPerThread; ++batch) {
        std::array<std::string, kEntriesPerBatch> keys;
        std::array<std::vector<std::byte>, kEntriesPerBatch> values;
        std::array<LumoDB::StructEntry, kEntriesPerBatch> entries;
        for (uint32_t entry = 0; entry < kEntriesPerBatch; ++entry) {
          keys[entry] = "key-" + std::to_string(threadId) + "-" + std::to_string(batch) + "-" +
                        std::to_string(entry);
          values[entry] = LumoDB::test::MakeBytes("value-" + std::to_string(batch) + "-" +
                                                  std::to_string(entry));
          entries[entry] = {.key = keys[entry], .flatBufferBytes = values[entry]};
        }
        if (!database.PutStructs("parallel-batches", entries)) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  ASSERT_FALSE(failed.load(std::memory_order_relaxed));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), kEntryCount);

  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    std::vector<std::byte> value;
    ASSERT_OK(database.Get("parallel-batches",
                           "key-" + std::to_string(threadId) + "-99-3", value));
    EXPECT_EQ(LumoDB::test::BytesToString(value), "value-99-3");
  }
}

TEST(DatabaseTest, PutRowStructsIsThreadSafeAcrossConcurrentBatches) {
  const auto directory = LumoDB::test::MakeTestDirectory("parallel-row-struct-batches");
  constexpr uint32_t kThreads = 8;
  constexpr uint32_t kBatchesPerThread = 100;
  constexpr uint32_t kEntriesPerBatch = 4;
  constexpr uint32_t kEntryCount = kThreads * kBatchesPerThread * kEntriesPerBatch;
  LumoDB::DatabaseOptions options = TestOptions(kEntryCount);
  options.writerThreadCount = kThreads;
  options.expectedExplicitRowCount = kThreads;
  options.expectedExplicitEntryCount = kEntryCount;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    threads.emplace_back([&, threadId] {
      for (uint32_t batch = 0; batch < kBatchesPerThread; ++batch) {
        std::array<std::string, kEntriesPerBatch> keys;
        std::array<std::vector<std::byte>, kEntriesPerBatch> values;
        std::array<LumoDB::RowStructEntry, kEntriesPerBatch> entries;
        for (uint32_t entry = 0; entry < kEntriesPerBatch; ++entry) {
          keys[entry] = "key-" + std::to_string(threadId) + "-" + std::to_string(batch) + "-" +
                        std::to_string(entry);
          values[entry] = LumoDB::test::MakeBytes("value-" + std::to_string(batch) + "-" +
                                                  std::to_string(entry));
          entries[entry] = {.key = keys[entry], .flatBufferBytes = values[entry]};
        }
        if (!database.PutRowStructs("parallel-batches", threadId, entries)) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  ASSERT_FALSE(failed.load(std::memory_order_relaxed));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), kEntryCount);
  EXPECT_EQ(database.RowCount(), kThreads);

  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    std::vector<std::byte> value;
    ASSERT_OK(database.Get("parallel-batches", threadId,
                           "key-" + std::to_string(threadId) + "-99-3", value));
    EXPECT_EQ(LumoDB::test::BytesToString(value), "value-99-3");
  }
}

TEST(DatabaseTest, RowIndexGrowsWhenColumnEstimateIsExceeded) {
  const auto directory = LumoDB::test::MakeTestDirectory("index-growth");
  LumoDB::DatabaseOptions options = TestOptions(1'000);
  options.expectedAutomaticColumnCount = 1;
  options.expectedExplicitRowCount = 0;
  options.maxEntriesPerRow = 768;
  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  const uint64_t originalBuckets = database.Layout().rowIndexBucketCount;

  for (uint32_t column = 0; column < 100; ++column) {
    ASSERT_OK(database.Put("column-" + std::to_string(column), "key", "value"));
  }
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), 100);
  EXPECT_GT(database.Layout().rowIndexBucketCount, originalBuckets);

  std::string value;
  ASSERT_OK(database.Get("column-99", "key", value));
  EXPECT_EQ(value, "value");
}

#if !defined(_WIN32)
TEST(DatabaseTest, InterruptedBuildIsRejected) {
  const auto directory = LumoDB::test::MakeTestDirectory("incomplete-build");
  LumoDB::DatabaseOptions options = TestOptions(5'000);
  options.stageBufferBytes = 4 * 1024;
  options.rowShardCount = 2;
  {
    LumoDB::Database database;
    ASSERT_OK(database.Open(directory, options));
    ASSERT_OK(database.Put("column", "committed", "base"));
    ASSERT_OK(database.Flush());
    ASSERT_OK(database.Close());
  }

  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    LumoDB::Database database;
    if (!database.Open(directory, options)) {
      _exit(2);
    }
    for (uint32_t index = 0; index < 2'000; ++index) {
      if (!database.Put("column", "uncommitted-" + std::to_string(index), "payload")) {
        _exit(3);
      }
    }
    _exit(0);  // Deliberately bypass destructors and Close().
  }
  int childStatus = 0;
  ASSERT_EQ(waitpid(child, &childStatus, 0), child);
  ASSERT_TRUE(WIFEXITED(childStatus));
  ASSERT_EQ(WEXITSTATUS(childStatus), 0);
  ASSERT_TRUE(std::filesystem::exists(directory / "build.incomplete"));

  LumoDB::Database rejected;
  const LumoDB::Status writableStatus = rejected.Open(directory, options);
  EXPECT_EQ(writableStatus.Code(), LumoDB::StatusCode::kCorruption);
  EXPECT_NE(writableStatus.Message().find("incomplete"), std::string::npos);
  const LumoDB::Status readOnlyStatus = rejected.OpenReadOnly(directory);
  EXPECT_EQ(readOnlyStatus.Code(), LumoDB::StatusCode::kCorruption);
}
#endif

TEST(StorageFormatTest, HotBucketsAreCompact) {
  EXPECT_EQ(sizeof(LumoDB::detail::RowKeyBucket), 24);
  EXPECT_EQ(sizeof(LumoDB::detail::RowIndexBucket), 48);
}

TEST(DatabaseTest, ReportsInvalidStateAndArguments) {
  LumoDB::Database database;
  std::string value;
  const std::vector<std::byte> rowValue = LumoDB::test::MakeBytes("row-value");
  const std::array<LumoDB::RowStructEntry, 1> validRowEntries = {
      LumoDB::RowStructEntry{.key = "key", .flatBufferBytes = rowValue},
  };
  const std::array<LumoDB::StructEntry, 1> validEntries = {
      LumoDB::StructEntry{.key = "key", .flatBufferBytes = rowValue},
  };
  EXPECT_EQ(database.Get("column", "key", value).Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.Put("column", "key", "value").Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.PutStructs("column", validEntries).Code(),
            LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.PutRowStructs("column", 0, validRowEntries).Code(),
            LumoDB::StatusCode::kNotOpen);
  std::vector<std::byte> bytes;
  EXPECT_EQ(database.GetRowStruct("column", 0, "key", bytes).Code(),
            LumoDB::StatusCode::kNotOpen);

  const auto directory = LumoDB::test::MakeTestDirectory("arguments");
  ASSERT_OK(database.Open(directory, TestOptions(100)));
  EXPECT_EQ(database.Put("", "key", "value").Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Put("column", "", "value").Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Put("column", 1ULL << 63, "key", "value").Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Get("column", "missing", value).Code(), LumoDB::StatusCode::kNotFound);
  ASSERT_OK(database.PutStructs("column", {}));
  EXPECT_EQ(database.PutStructs("", validEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  const std::array<LumoDB::StructEntry, 2> partiallyInvalidAutomaticEntries = {
      validEntries.front(),
      LumoDB::StructEntry{.key = "", .flatBufferBytes = rowValue},
  };
  EXPECT_EQ(database.PutStructs("column", partiallyInvalidAutomaticEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.PutRowStructs("column", 0, {}).Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.PutRowStructs("", 0, validRowEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.PutRowStructs("column", 1ULL << 63, validRowEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  const std::array<LumoDB::RowStructEntry, 2> partiallyInvalidEntries = {
      validRowEntries.front(),
      LumoDB::RowStructEntry{.key = "", .flatBufferBytes = rowValue},
  };
  EXPECT_EQ(database.PutRowStructs("column", 0, partiallyInvalidEntries).Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_FALSE(database.HasUncommittedWrites());

  LumoDB::DatabaseOptions nonFiniteOptions = TestOptions(100);
  nonFiniteOptions.maxLoadFactor = std::numeric_limits<double>::quiet_NaN();
  LumoDB::Database nonFiniteDatabase;
  EXPECT_EQ(nonFiniteDatabase
                .Open(LumoDB::test::MakeTestDirectory("non-finite-load-factor"), nonFiniteOptions)
                .Code(),
            LumoDB::StatusCode::kInvalidArgument);
}

}  // namespace
