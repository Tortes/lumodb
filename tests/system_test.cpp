#include <atomic>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "test_utils.h"

namespace {

TEST(SystemTest, BuildsAndRandomlyReadsAParallelDataSet) {
  constexpr uint32_t kEntryCount = 200'000;
  constexpr uint32_t kThreads = 8;
  const auto directory = LumoDB::test::MakeTestDirectory("system-parallel");

  LumoDB::DatabaseOptions options;
  options.expectedEntryCountPerColumn = kEntryCount;
  options.averageKeyBytes = 16;
  options.averageValueBytes = 32;
  options.maxEntriesPerRow = 3'072;
  options.writerThreadCount = kThreads;
  options.rowShardCount = kThreads;
  options.memoryBudgetBytes = 256ULL * 1024 * 1024;
  options.stageBufferBytes = 64 * 1024;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  const LumoDB::DatabaseLayout layout = database.Layout();

  std::atomic<bool> failed{false};
  std::vector<std::thread> writers;
  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    writers.emplace_back([&, threadId] {
      for (uint32_t index = threadId; index < kEntryCount; index += kThreads) {
        const std::string key = "key-" + std::to_string(index);
        const std::string value = "value-" + std::to_string(index);
        if (!database.Put("objects", key, value)) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (std::thread& writer : writers) {
    writer.join();
  }
  ASSERT_FALSE(failed.load(std::memory_order_relaxed));
  ASSERT_OK(database.Flush());
  EXPECT_EQ(database.EntryCount(), kEntryCount);
  EXPECT_GT(database.RowCount(), layout.routeCountPerColumn / 2);
  EXPECT_LE(database.RowCount(), layout.routeCountPerColumn);

  std::mt19937 random(12345);
  for (uint32_t sample = 0; sample < 2'000; ++sample) {
    const uint32_t index = random() % kEntryCount;
    std::string value;
    ASSERT_OK(database.Get("objects", "key-" + std::to_string(index), value));
    EXPECT_EQ(value, "value-" + std::to_string(index));
  }
  ASSERT_OK(database.Close());

  ASSERT_OK(database.OpenReadOnly(directory));
  failed.store(false, std::memory_order_relaxed);
  std::vector<std::thread> readers;
  for (uint32_t threadId = 0; threadId < kThreads; ++threadId) {
    readers.emplace_back([&, threadId] {
      for (uint32_t sample = threadId; sample < 8'000; sample += kThreads) {
        const uint32_t index = (sample * 104'729U) % kEntryCount;
        std::string value;
        const LumoDB::Status status =
            database.Get("objects", "key-" + std::to_string(index), value);
        if (!status || value != "value-" + std::to_string(index)) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (std::thread& reader : readers) {
    reader.join();
  }
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
}

}  // namespace
