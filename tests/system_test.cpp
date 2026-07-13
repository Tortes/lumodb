#include "test_utils.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

class ThreadFailures {
 public:
  void Add(std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (messages_.size() < kMaxMessages) {
      messages_.push_back(std::move(message));
    }
    ++count_;
  }

  bool Empty() {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_ == 0;
  }

  std::string Report() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ostringstream output;
    output << count_ << " failures";
    for (const std::string& message : messages_) {
      output << "\n" << message;
    }
    return output.str();
  }

 private:
  static constexpr size_t kMaxMessages = 16;

  std::mutex mutex_;
  std::vector<std::string> messages_;
  size_t count_ = 0;
};

std::string ObjectKey(uint64_t index) {
  return "object-" + std::to_string(index);
}

std::string ObjectValue(std::string_view column, uint64_t index) {
  return "object:" + std::string(column) + ":" + std::to_string(index);
}

std::string RowKey(uint64_t keyIndex) {
  return "field-" + std::to_string(keyIndex);
}

std::string RowValue(uint64_t rowId, uint64_t keyIndex) {
  return "row:" + std::to_string(rowId) + ":field:" + std::to_string(keyIndex);
}

struct RowPayload {
  std::vector<std::string> keys;
  std::vector<std::vector<std::byte>> values;
  std::vector<LumoDB::RowStructEntry> entries;
};

RowPayload BuildRowPayload(uint64_t rowId, uint64_t entryCount) {
  RowPayload payload;
  payload.keys.reserve(static_cast<size_t>(entryCount));
  payload.values.reserve(static_cast<size_t>(entryCount));
  payload.entries.reserve(static_cast<size_t>(entryCount));

  for (uint64_t keyIndex = 0; keyIndex < entryCount; ++keyIndex) {
    payload.keys.push_back(RowKey(keyIndex));
    payload.values.push_back(LumoDB::test::MakeBytes(RowValue(rowId, keyIndex)));
    payload.entries.push_back({
        .key = payload.keys.back(),
        .flatBufferBytes = payload.values.back(),
    });
  }

  return payload;
}

void ExpectRowValue(const LumoDB::Database& database, std::string_view column,
                    uint64_t rowId, uint64_t keyIndex) {
  std::vector<std::byte> value;
  ASSERT_OK(database.GetRowStruct(column, rowId, RowKey(keyIndex), value));
  EXPECT_EQ(LumoDB::test::BytesToString(value), RowValue(rowId, keyIndex));
}

void VerifyRowsInParallel(const LumoDB::Database& database, std::string_view column,
                          uint64_t rowCount, uint64_t entryCount,
                          uint32_t threadCount, ThreadFailures& failures) {
  std::atomic<uint64_t> nextRow{0};
  std::vector<std::thread> readers;
  readers.reserve(threadCount);

  for (uint32_t threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
    readers.emplace_back([&]() {
      for (;;) {
        const uint64_t rowId = nextRow.fetch_add(1, std::memory_order_relaxed);
        if (rowId >= rowCount) {
          return;
        }

        for (uint64_t keyIndex = 0; keyIndex < entryCount; ++keyIndex) {
          std::vector<std::byte> value;
          LumoDB::Status status =
              database.GetRowStruct(column, rowId, RowKey(keyIndex), value);
          if (!status) {
            failures.Add("read row " + std::to_string(rowId) + " key " +
                         std::to_string(keyIndex) + " failed: " + status.Message());
            continue;
          }

          const std::string actual = LumoDB::test::BytesToString(value);
          const std::string expected = RowValue(rowId, keyIndex);
          if (actual != expected) {
            failures.Add("read row " + std::to_string(rowId) + " key " +
                         std::to_string(keyIndex) + " expected " + expected +
                         " got " + actual);
          }
        }
      }
    });
  }

  for (std::thread& reader : readers) {
    reader.join();
  }
}

void VerifyObjectsInParallel(const LumoDB::Database& database, std::string_view column,
                             uint64_t objectCount, uint32_t threadCount,
                             ThreadFailures& failures) {
  std::atomic<uint64_t> nextObject{0};
  std::vector<std::thread> readers;
  readers.reserve(threadCount);

  for (uint32_t threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
    readers.emplace_back([&]() {
      for (;;) {
        const uint64_t objectIndex =
            nextObject.fetch_add(1, std::memory_order_relaxed);
        if (objectIndex >= objectCount) {
          return;
        }

        std::vector<std::byte> value;
        LumoDB::Status status =
            database.GetStruct(column, ObjectKey(objectIndex), value);
        if (!status) {
          failures.Add("read object " + std::to_string(objectIndex) +
                       " failed: " + status.Message());
          continue;
        }

        const std::string actual = LumoDB::test::BytesToString(value);
        const std::string expected = ObjectValue(column, objectIndex);
        if (actual != expected) {
          failures.Add("read object " + std::to_string(objectIndex) +
                       " expected " + expected + " got " + actual);
        }
      }
    });
  }

  for (std::thread& reader : readers) {
    reader.join();
  }
}

TEST(LumoDBSystemTest, WritesReadsObjectPayloadsAndReopens) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("st-object-write-read");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 256;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kObjectCount = 32;
  constexpr std::array<std::string_view, 3> kColumns = {
      "StructA",
      "StructB",
      "StructC",
  };

  for (std::string_view column : kColumns) {
    for (uint64_t index = 0; index < kObjectCount; ++index) {
      ASSERT_OK(database.PutStruct(column, ObjectKey(index),
                                   LumoDB::test::MakeBytes(ObjectValue(column, index))));
    }
  }

  for (std::string_view column : kColumns) {
    for (uint64_t index = 0; index < kObjectCount; ++index) {
      std::vector<std::byte> value;
      ASSERT_OK(database.GetStruct(column, ObjectKey(index), value));
      EXPECT_EQ(LumoDB::test::BytesToString(value), ObjectValue(column, index));
    }
  }

  std::vector<std::string> keys;
  keys.reserve(kObjectCount);
  for (uint64_t index = 0; index < kObjectCount; ++index) {
    keys.push_back(ObjectKey(index));
  }

  std::vector<std::vector<std::byte>> values;
  ASSERT_OK(database.GetMany("StructB", keys, values));
  ASSERT_EQ(values.size(), keys.size());
  for (uint64_t index = 0; index < kObjectCount; ++index) {
    EXPECT_EQ(LumoDB::test::BytesToString(values[static_cast<size_t>(index)]),
              ObjectValue("StructB", index));
  }

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));

  for (std::string_view column : kColumns) {
    for (uint64_t index = 0; index < kObjectCount; ++index) {
      std::vector<std::byte> value;
      ASSERT_OK(database.GetStruct(column, ObjectKey(index), value));
      EXPECT_EQ(LumoDB::test::BytesToString(value), ObjectValue(column, index));
    }
  }
  ASSERT_OK(database.Close());
}

TEST(LumoDBSystemTest, WritesReadsRowPayloadsAndReopens) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("st-row-write-read");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 128;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kRowCount = 24;
  constexpr uint64_t kEntryCount = 6;

  for (uint64_t rowId = 0; rowId < kRowCount; ++rowId) {
    RowPayload payload = BuildRowPayload(rowId, kEntryCount);
    ASSERT_OK(database.PutRowStructs("StructA", rowId, payload.entries));
  }

  EXPECT_EQ(database.RowCount(), kRowCount);
  for (uint64_t rowId = 0; rowId < kRowCount; ++rowId) {
    for (uint64_t keyIndex = 0; keyIndex < kEntryCount; ++keyIndex) {
      ExpectRowValue(database, "StructA", rowId, keyIndex);
    }
  }

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));

  EXPECT_EQ(database.RowCount(), kRowCount);
  for (uint64_t rowId = 0; rowId < kRowCount; ++rowId) {
    for (uint64_t keyIndex = 0; keyIndex < kEntryCount; ++keyIndex) {
      ExpectRowValue(database, "StructA", rowId, keyIndex);
    }
  }
  ASSERT_OK(database.Close());
}

TEST(LumoDBSystemTest, WritesCompleteRowsOnceThenFlushesAndReopens) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("st-complete-row-write");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 128;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kRowCount = 48;
  constexpr uint64_t kEntryCount = 16;
  for (uint64_t rowId = 0; rowId < kRowCount; ++rowId) {
    // Build every key/value first, then append this row exactly once.
    RowPayload payload = BuildRowPayload(rowId, kEntryCount);
    ASSERT_OK(database.PutRowStructs("StructA", rowId, payload.entries));
  }
  ASSERT_OK(database.Flush());

  EXPECT_EQ(database.RowCount(), kRowCount);
  std::vector<LumoDB::ColumnStats> stats;
  ASSERT_OK(database.GetColumnStats(stats));
  ASSERT_EQ(stats.size(), 1);
  EXPECT_EQ(stats[0].column, "StructA");
  EXPECT_EQ(stats[0].objectCount, kRowCount * kEntryCount);
  EXPECT_EQ(stats[0].rowCount, kRowCount);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));
  for (uint64_t rowId = 0; rowId < kRowCount; ++rowId) {
    for (uint64_t keyIndex = 0; keyIndex < kEntryCount; ++keyIndex) {
      ExpectRowValue(database, "StructA", rowId, keyIndex);
    }
  }
  ASSERT_OK(database.Close());
}

TEST(LumoDBSystemTest, WritesSameColumnRowsInParallelAndReadsRowsInParallel) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("st-row-parallel-read-write");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 1024;
  options.rowShardCount = 8;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  constexpr uint64_t kRowCount = 512;
  constexpr uint64_t kEntryCount = 8;
  constexpr uint32_t kWriterCount = 8;
  constexpr uint32_t kReaderCount = 8;

  std::atomic<uint64_t> nextRow{0};
  ThreadFailures writeFailures;
  std::vector<std::thread> writers;
  writers.reserve(kWriterCount);

  for (uint32_t threadIndex = 0; threadIndex < kWriterCount; ++threadIndex) {
    writers.emplace_back([&]() {
      for (;;) {
        const uint64_t rowId = nextRow.fetch_add(1, std::memory_order_relaxed);
        if (rowId >= kRowCount) {
          return;
        }

        RowPayload payload = BuildRowPayload(rowId, kEntryCount);
        LumoDB::Status status =
            database.PutRowStructs("StructA", rowId, payload.entries);
        if (!status) {
          writeFailures.Add("write row " + std::to_string(rowId) +
                            " failed: " + status.Message());
        }
      }
    });
  }

  for (std::thread& writer : writers) {
    writer.join();
  }

  ASSERT_TRUE(writeFailures.Empty()) << writeFailures.Report();
  ASSERT_EQ(database.RowCount(), kRowCount);

  ThreadFailures readFailures;
  VerifyRowsInParallel(database, "StructA", kRowCount, kEntryCount, kReaderCount,
                       readFailures);
  EXPECT_TRUE(readFailures.Empty()) << readFailures.Report();

  ASSERT_OK(database.Close());
  ASSERT_OK(database.Open(directory, options));

  ThreadFailures reopenReadFailures;
  VerifyRowsInParallel(database, "StructA", kRowCount, kEntryCount, kReaderCount,
                       reopenReadFailures);
  EXPECT_TRUE(reopenReadFailures.Empty()) << reopenReadFailures.Report();
  ASSERT_OK(database.Close());
}

TEST(LumoDBSystemTest, UsesImmutableWriteAndReadOnlyWorkflow) {
  const std::filesystem::path directory =
      LumoDB::test::MakeTestDirectory("st-best-performance-workflow");

  constexpr uint64_t kObjectCount = 192;
  constexpr uint64_t kRowCount = 96;
  constexpr uint64_t kRowEntryCount = 12;
  constexpr uint32_t kWriterCount = 4;
  constexpr uint32_t kReaderCount = 4;

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 256;
  options.initialRowBucketCount = 128;
  options.rowShardCount = kWriterCount;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::vector<std::string> objectKeys;
  std::vector<std::vector<std::byte>> objectValues;
  std::vector<LumoDB::StructEntry> objectEntries;
  objectKeys.reserve(kObjectCount);
  objectValues.reserve(kObjectCount);
  objectEntries.reserve(kObjectCount);
  for (uint64_t objectIndex = 0; objectIndex < kObjectCount; ++objectIndex) {
    objectKeys.push_back(ObjectKey(objectIndex));
    objectValues.push_back(
        LumoDB::test::MakeBytes(ObjectValue("FlatColumn", objectIndex)));
    objectEntries.push_back({.key = objectKeys.back(),
                             .flatBufferBytes = objectValues.back()});
  }
  ASSERT_OK(database.PutUniqueStructs("FlatColumn", objectEntries));

  std::atomic<uint64_t> nextRow{0};
  ThreadFailures writeFailures;
  std::vector<std::thread> writers;
  writers.reserve(kWriterCount);
  for (uint32_t writerIndex = 0; writerIndex < kWriterCount; ++writerIndex) {
    writers.emplace_back([&]() {
      for (;;) {
        const uint64_t rowId = nextRow.fetch_add(1, std::memory_order_relaxed);
        if (rowId >= kRowCount) {
          return;
        }

        RowPayload payload = BuildRowPayload(rowId, kRowEntryCount);
        LumoDB::Status status =
            database.PutRowStructs("RowColumn", rowId, payload.entries);
        if (!status) {
          writeFailures.Add("write row " + std::to_string(rowId) +
                            " failed: " + status.Message());
        }
      }
    });
  }
  for (std::thread& writer : writers) {
    writer.join();
  }
  ASSERT_TRUE(writeFailures.Empty()) << writeFailures.Report();
  EXPECT_EQ(database.EntryCount(), kObjectCount);
  EXPECT_EQ(database.RowCount(), kRowCount);

  ASSERT_OK(database.Close());
  ASSERT_OK(database.OpenReadOnly(directory, options));

  ThreadFailures objectReadFailures;
  VerifyObjectsInParallel(database, "FlatColumn", kObjectCount, kReaderCount,
                          objectReadFailures);
  EXPECT_TRUE(objectReadFailures.Empty()) << objectReadFailures.Report();

  ThreadFailures rowReadFailures;
  VerifyRowsInParallel(database, "RowColumn", kRowCount, kRowEntryCount,
                       kReaderCount, rowReadFailures);
  EXPECT_TRUE(rowReadFailures.Empty()) << rowReadFailures.Report();
  ASSERT_OK(database.Close());
}

}  // namespace
