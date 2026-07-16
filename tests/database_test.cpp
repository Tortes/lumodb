#include <atomic>
#include <bit>
#include <filesystem>
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
  EXPECT_EQ(database.Get("column", "key", value).Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.Put("column", "key", "value").Code(), LumoDB::StatusCode::kNotOpen);

  const auto directory = LumoDB::test::MakeTestDirectory("arguments");
  ASSERT_OK(database.Open(directory, TestOptions(100)));
  EXPECT_EQ(database.Put("", "key", "value").Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Put("column", "", "value").Code(), LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Put("column", 1ULL << 63, "key", "value").Code(),
            LumoDB::StatusCode::kInvalidArgument);
  EXPECT_EQ(database.Get("column", "missing", value).Code(), LumoDB::StatusCode::kNotFound);
}

}  // namespace
